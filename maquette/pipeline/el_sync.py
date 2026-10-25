"""
============================================================
VOIE LOTS — ingestion des référentiels (rôle d'Airbyte dans la maquette)
------------------------------------------------------------
Les données qui changent lentement restent chargées par lots, comme le
ferait un connecteur Airbyte en mode « Full refresh | Overwrite » :

  source                        destination          mode Airbyte équivalent
  Données de référence (CSV)    entrepôt, couche raw  Full refresh | Overwrite

Les transactions, clients et commerçants n'y passent plus : ils arrivent en
continu par la voie flux (Debezium -> Kafka -> pipeline/stream_processor.py).
Aucune transformation métier ici : on charge le brut, dbt transforme ensuite (ELT).

Gestion des erreurs :
  - chaque ligne est contrôlée ; une ligne invalide part en quarantaine
    (raw._quarantine) avec sa raison, les autres sont chargées ;
  - si plus de 10 % des lignes d'un fichier sont invalides, le fichier entier
    est refusé : l'ancienne version reste en place (jamais de référentiel
    remplacé par un fichier abîmé) et la tâche échoue (alerte).
============================================================
"""
from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Callable

import psycopg2
from psycopg2.extras import execute_values

from pipeline.config import env
from pipeline.errors import InvalidRecord

MAX_REJECTED_SHARE = 0.10


def _country(row: dict) -> tuple:
    code, name = (row.get("country_code") or "").strip(), (row.get("name") or "").strip()
    if not re.fullmatch(r"[A-Z]{2}", code):
        raise InvalidRecord(f"country_code invalide : {code!r}")
    if not name:
        raise InvalidRecord("name vide")
    return code, name


def _exchange_rate(row: dict) -> tuple:
    currency = (row.get("currency_code") or "").strip()
    if not re.fullmatch(r"[A-Z]{3}", currency):
        raise InvalidRecord(f"currency_code invalide : {currency!r}")
    try:
        rate_date = date.fromisoformat((row.get("rate_date") or "").strip())
    except ValueError as exc:
        raise InvalidRecord(f"rate_date invalide : {row.get('rate_date')!r}") from exc
    try:
        rate = float(row.get("rate_to_usd") or "")
    except ValueError as exc:
        raise InvalidRecord(f"rate_to_usd invalide : {row.get('rate_to_usd')!r}") from exc
    if rate <= 0:
        raise InvalidRecord(f"rate_to_usd doit être positif : {rate}")
    return currency, rate_date, rate


@dataclass(frozen=True)
class Stream:
    name: str
    mode: str                  # "full_refresh"
    columns: tuple[str, ...]
    validate: Callable[[dict], tuple]


REFERENCE_FILES = (
    Stream("countries", "full_refresh", ("country_code", "name"), _country),
    Stream("exchange_rates", "full_refresh", ("currency_code", "rate_date", "rate_to_usd"), _exchange_rate),
)


class ReferenceFileRejected(Exception):
    """Trop de lignes invalides : le fichier n'est pas chargé, l'ancienne version reste en place."""


def validate_file(stream: Stream, records: list[dict]) -> tuple[list[tuple], list[tuple[int, dict, str]]]:
    """Sépare les lignes valides des lignes invalides (numéro de ligne, contenu, raison)."""
    valid, rejected = [], []
    for line_number, record in enumerate(records, start=2):     # ligne 1 = en-tête
        try:
            valid.append(stream.validate(record))
        except InvalidRecord as exc:
            rejected.append((line_number, record, str(exc)))
    return valid, rejected


def _quarantine(wh_cur, stream: Stream, rejected: list[tuple[int, dict, str]]) -> None:
    """Une même ligne fautive n'est mise en quarantaine qu'une fois, même si le fichier est relu
    toutes les 5 minutes tant qu'il n'est pas corrigé."""
    if rejected:
        execute_values(wh_cur,
                       """INSERT INTO raw._quarantine (source, location, payload, error)
                          SELECT v.source, v.location, v.payload, v.error
                          FROM (VALUES %s) AS v(source, location, payload, error)
                          WHERE NOT EXISTS (SELECT 1 FROM raw._quarantine q
                                            WHERE q.source = v.source AND q.location = v.location
                                              AND q.payload = v.payload AND q.replayed_at IS NULL)""",
                       [(f"reference/{stream.name}.csv", f"ligne {n}", str(record), reason)
                        for n, record, reason in rejected])


def _save_state(wh_cur, stream: str, mode: str, rows: int) -> None:
    """État de chaque flux, comme le « state » d'un connecteur Airbyte (lu par le contrôle de fraîcheur)."""
    wh_cur.execute(
        """INSERT INTO raw._sync_state (stream, sync_mode, cursor_value, rows_synced, synced_at)
           VALUES (%s, %s, NULL, %s, now())
           ON CONFLICT (stream) DO UPDATE
           SET sync_mode = EXCLUDED.sync_mode, cursor_value = NULL,
               rows_synced = EXCLUDED.rows_synced, synced_at = now()""",
        (stream, mode, rows),
    )


def _overwrite(wh_cur, stream: Stream, rows: list[tuple]) -> None:
    wh_cur.execute(f"TRUNCATE raw.{stream.name}")
    if rows:
        execute_values(wh_cur, f"INSERT INTO raw.{stream.name} ({', '.join(stream.columns)}) VALUES %s", rows)


def sync_reference() -> dict:
    """Recharge chaque référentiel en une transaction : en cas d'échec, l'ancienne version
    reste en place (jamais de table à moitié vide). Renvoie le nombre de lignes chargées et
    mises en quarantaine par fichier."""
    ref_dir = Path(env("REFERENCE_DIR"))
    dst = psycopg2.connect(env("WH_LOADER_DSN"))
    report = {}
    try:
        for stream in REFERENCE_FILES:
            with (ref_dir / f"{stream.name}.csv").open(encoding="utf-8", newline="") as fh:
                records = list(csv.DictReader(fh))
            valid, rejected = validate_file(stream, records)
            with dst.cursor() as cur:            # la quarantaine est gardée, même si le fichier est refusé
                _quarantine(cur, stream, rejected)
            dst.commit()
            if not records or len(rejected) > MAX_REJECTED_SHARE * len(records):
                raise ReferenceFileRejected(
                    f"{stream.name}.csv : {len(rejected)} ligne(s) invalide(s) sur {len(records)} "
                    f"(seuil {MAX_REJECTED_SHARE:.0%}) ; ancienne version conservée")
            with dst.cursor() as cur:
                _overwrite(cur, stream, valid)
                _save_state(cur, f"reference.{stream.name}", stream.mode, len(valid))
            dst.commit()
            report[stream.name] = {"chargées": len(valid), "en_quarantaine": len(rejected)}
    except Exception:
        dst.rollback()
        raise
    finally:
        dst.close()
    print(f"Référence -> raw : {report}")
    return report
