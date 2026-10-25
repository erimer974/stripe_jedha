"""
============================================================
GESTION DES ERREURS — deux familles, deux réponses (Livrable 5 §11)
------------------------------------------------------------
  erreur PASSAGÈRE  (la base ou le réseau est indisponible) :
      réessayer, avec une attente qui double à chaque échec ;
      la donnée n'est pas en cause, rien n'est jeté.
  erreur DÉFINITIVE (la donnée elle-même est invalide) :
      réessayer ne servirait à rien et bloquerait tout le flux ;
      l'enregistrement part en QUARANTAINE (raw._quarantine) avec
      sa raison, et le flux continue.
============================================================
"""
from __future__ import annotations

import psycopg2
from pymongo.errors import AutoReconnect, NetworkTimeout, ServerSelectionTimeoutError


class InvalidRecord(Exception):
    """Erreur définitive : l'enregistrement ne peut pas être chargé tel quel."""


# Indisponibilités : on réessaie. Tout le reste (données refusées par une contrainte,
# type incorrect, document hors schéma) est traité comme une erreur définitive.
TRANSIENT_ERRORS = (
    psycopg2.OperationalError,
    psycopg2.InterfaceError,
    AutoReconnect,
    NetworkTimeout,
    ServerSelectionTimeoutError,
    ConnectionError,
    TimeoutError,
)


def is_transient(exc: BaseException) -> bool:
    return isinstance(exc, TRANSIENT_ERRORS)
