"""
============================================================
GARDIEN DU CLUSTER OLTP — rôle de Patroni dans la maquette (Livrable 5 §9)
------------------------------------------------------------
Chaque seconde :
  1. vérifie que le primaire répond ;
  2. réplication synchrone tant que le réplica est présent (RPO 0) ; s'il
     disparaît, passage temporaire en asynchrone pour ne pas bloquer les
     paiements (mode dégradé, signalé), retour au synchrone à son retour ;
  3. après 3 échecs consécutifs du primaire (≈ 3 à 6 s), BASCULE : le réplica
     est promu primaire, la configuration synchrone est remise à zéro, et le
     fichier /ha/leader désigne le nouveau primaire ;
  4. isole (« fencing ») un ancien primaire qui répondrait encore : il passe
     en lecture seule et ses connexions sont coupées. Redémarré, il se
     reconstruit en réplica (ha/node-entrypoint.sh).

Usage : python /opt/ha/failover_watchdog.py
============================================================
"""
from __future__ import annotations

import json
import os
import socket
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, contextmanager
from datetime import datetime

import psycopg2

NODES = ("oltp-db", "oltp-replica")
LEADER_FILE = "/ha/leader"
CHECK_EVERY_S = 1
FAILURES_TO_FAILOVER = 3
MISSING_CHECKS_TO_DEGRADE = 3


def log(message: str) -> None:
    print(f"[gardien {datetime.now():%H:%M:%S}] {message}", flush=True)


def alert(severity: str, message: str) -> None:
    """Journalise et historise l'alerte dans warehouse.audit.alerts (rôle alerter) ; ne bloque jamais."""
    log(f"ALERTE {severity} : {message}")
    dsn = os.environ.get("ALERTS_DSN")
    if not dsn:
        return
    try:
        with closing(psycopg2.connect(dsn, connect_timeout=2)) as conn, conn.cursor() as cur:
            cur.execute("INSERT INTO audit.alerts (severity, source, message) VALUES (%s, 'gardien-oltp', %s)",
                        (severity, message))
            conn.commit()
    except psycopg2.Error:
        pass


def slot_of(node: str) -> str:
    return node.replace("-", "_")


def other(node: str) -> str:
    return NODES[1] if node == NODES[0] else NODES[0]


def read_leader() -> str:
    try:
        with open(LEADER_FILE, encoding="utf-8") as fh:
            return fh.read().strip() or os.environ["INITIAL_LEADER"]
    except FileNotFoundError:
        return os.environ["INITIAL_LEADER"]


def write_leader(node: str) -> None:
    tmp = LEADER_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        fh.write(node)
    os.replace(tmp, LEADER_FILE)


_RESOLVER = ThreadPoolExecutor(max_workers=2)
_KNOWN_IPS: dict[str, str] = {}
_REFRESHING: set[str] = set()


def resolve(host: str) -> str:
    """Adresse IP du nœud. La résolution d'un nom de conteneur peut prendre plusieurs secondes
    (surtout s'il est arrêté) : on garde la dernière adresse connue, ce qui permet de constater
    une panne en 1 seconde (délai de connexion) au lieu d'attendre la résolution du nom."""
    if host in _KNOWN_IPS:
        # adresse connue : on l'utilise tout de suite, la mise à jour se fait en arrière-plan
        if host not in _REFRESHING:
            _REFRESHING.add(host)
            _RESOLVER.submit(_refresh_ip, host)
        return _KNOWN_IPS[host]
    try:
        _KNOWN_IPS[host] = _RESOLVER.submit(socket.gethostbyname, host).result(timeout=1.5)
    except Exception as exc:                  # noqa: BLE001
        raise psycopg2.OperationalError(f"{host} : nom introuvable ({exc!r})") from exc
    return _KNOWN_IPS[host]


def _refresh_ip(host: str) -> None:
    try:
        _KNOWN_IPS[host] = socket.gethostbyname(host)
    except OSError:
        pass                                  # conteneur arrêté : on garde l'ancienne adresse
    finally:
        _REFRESHING.discard(host)


def _connect_call(method: str, path: str) -> dict:
    request = urllib.request.Request(f"{os.environ['CONNECT_URL']}/connectors/oltp-cdc{path}", method=method)
    with urllib.request.urlopen(request, timeout=5) as resp:
        body = resp.read()
    return json.loads(body) if body else {}


def realign_cdc_on_slot() -> bool:
    """Après une bascule, la position mémorisée par Debezium vient de l'ANCIEN primaire et peut
    être un peu en avance sur le journal du nouveau : Debezium sauterait alors les premières
    transactions écrites après la bascule (constaté au test : 450 transactions). On efface cette
    position : Debezium repart du slot recopié, toujours en retard ou à égalité. Au pire, quelques
    changements sont relus, sans effet grâce aux écritures idempotentes ; aucun n'est sauté."""
    if not os.environ.get("CONNECT_URL"):
        return True
    try:
        _connect_call("PUT", "/stop")
        for _ in range(30):
            if _connect_call("GET", "/status")["connector"]["state"] == "STOPPED":
                break
            time.sleep(1)
        _connect_call("DELETE", "/offsets")
        # sans position mémorisée, Debezium repart du slot au lieu de tout relire (chargement initial)
        config = _connect_call("GET", "/config")
        if config.get("snapshot.mode") != "no_data":
            config["snapshot.mode"] = "no_data"
            body = json.dumps(config).encode()
            request = urllib.request.Request(f"{os.environ['CONNECT_URL']}/connectors/oltp-cdc/config",
                                             data=body, method="PUT", headers={"Content-Type": "application/json"})
            urllib.request.urlopen(request, timeout=5).close()
        _connect_call("PUT", "/resume")
        log("capture des changements réalignée sur le slot du nouveau primaire (position effacée, reprise)")
        return True
    except (OSError, KeyError, ValueError) as exc:
        log(f"réalignement de la capture des changements impossible pour l'instant : {exc}")
        return False


def restart_failed_cdc_tasks() -> None:
    """Kafka Connect ne relance pas une tâche tombée au démarrage (par exemple pendant une
    bascule) : le gardien la relance, comme le ferait un opérateur Kubernetes (Strimzi)."""
    url = os.environ.get("CONNECT_URL")
    if not url:
        return
    try:
        with urllib.request.urlopen(f"{url}/connectors/oltp-cdc/status", timeout=2) as resp:
            status = json.load(resp)
        if any(task["state"] == "FAILED" for task in status.get("tasks", [])):
            request = urllib.request.Request(
                f"{url}/connectors/oltp-cdc/restart?includeTasks=true&onlyFailed=true", method="POST")
            urllib.request.urlopen(request, timeout=5).close()
            alert("WARNING", "tâche de capture des changements (Debezium) en échec : relancée")
    except OSError:
        pass                                  # Kafka Connect indisponible : on réessaiera


@contextmanager
def connect(host: str):
    conn = psycopg2.connect(host=host, hostaddr=resolve(host), dbname="stripe_oltp",
                            user=os.environ["PG_ADMIN_USER"], password=os.environ["PG_ADMIN_PASSWORD"],
                            connect_timeout=1)
    conn.autocommit = True        # ALTER SYSTEM ne s'exécute pas dans une transaction
    with closing(conn):
        yield conn


def set_synchronous(cur, standby_slot: str | None) -> None:
    """standby_slot = nom du réplica à attendre (RPO 0), ou None pour l'asynchrone."""
    if standby_slot:
        cur.execute(f"ALTER SYSTEM SET synchronous_standby_names = '{standby_slot}'")
        cur.execute(f"ALTER SYSTEM SET synchronized_standby_slots = '{standby_slot}'")
    else:
        cur.execute("ALTER SYSTEM RESET synchronous_standby_names")
        cur.execute("ALTER SYSTEM RESET synchronized_standby_slots")
    cur.execute("SELECT pg_reload_conf()")


class Watchdog:
    def __init__(self):
        self.failures = 0
        self.first_failure_at: float | None = None
        self.missing_standby = 0
        self.ticks = 0
        self.watch_old_leader_until = 0.0   # après une bascule : surveiller l'ancien primaire à chaque tour
        self.cdc_realign_pending = False

    def manage_replication(self, cur, standby: str) -> None:
        cur.execute("SELECT state FROM pg_stat_replication WHERE application_name = %s", (slot_of(standby),))
        row = cur.fetchone()
        streaming = row is not None and row[0] == "streaming"
        cur.execute("SHOW synchronous_standby_names")
        synchronous = cur.fetchone()[0] != ""
        if streaming:
            self.missing_standby = 0
            if not synchronous:
                set_synchronous(cur, slot_of(standby))
                log(f"réplica {standby} présent : réplication SYNCHRONE (RPO 0)")
        else:
            self.missing_standby += 1
            if synchronous and self.missing_standby >= MISSING_CHECKS_TO_DEGRADE:
                set_synchronous(cur, None)
                alert("WARNING", f"réplica {standby} absent : passage en ASYNCHRONE pour ne pas bloquer "
                      "les paiements (mode dégradé)")

    def fence(self, node: str) -> None:
        """Un ancien primaire qui répond encore ne doit plus accepter d'écritures."""
        try:
            with connect(node) as conn, conn.cursor() as cur:
                cur.execute("SELECT pg_is_in_recovery()")
                if cur.fetchone()[0]:
                    return                                   # c'est bien un réplica
                cur.execute("SHOW default_transaction_read_only")
                if cur.fetchone()[0] == "on":
                    return                                   # déjà isolé
                cur.execute("ALTER SYSTEM SET default_transaction_read_only = on")
                cur.execute("SELECT pg_reload_conf()")
                cur.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                            "WHERE pid <> pg_backend_pid() AND backend_type = 'client backend'")
                alert("WARNING", f"ancien primaire {node} isolé : lecture seule, connexions coupées "
                      "(redémarré, il se reconstruira en réplica)")
        except psycopg2.OperationalError:
            pass

    def failover(self, new_leader: str) -> None:
        try:
            with connect(new_leader) as conn, conn.cursor() as cur:
                cur.execute("SELECT pg_is_in_recovery()")
                if not cur.fetchone()[0]:
                    log(f"{new_leader} n'est pas un réplica : bascule impossible")
                    return
                promote_started = time.time()
                cur.execute("SELECT pg_promote(true, 30)")
                set_synchronous(cur, None)       # pas encore de réplica derrière le nouveau primaire
            write_leader(new_leader)
            self.watch_old_leader_until = time.time() + 120
            detection = promote_started - self.first_failure_at
            total = time.time() - self.first_failure_at
            alert("CRITICAL", f"BASCULE terminée : {new_leader} est le nouveau primaire. Détection {detection:.1f} s, "
                  f"promotion {total - detection:.1f} s, interruption des écritures ≈ {total:.1f} s")
            self.failures, self.first_failure_at, self.missing_standby = 0, None, 0
            self.cdc_realign_pending = not realign_cdc_on_slot()
        except psycopg2.OperationalError as exc:
            log(f"réplica {new_leader} injoignable, bascule impossible pour l'instant : {exc}")

    def tick(self) -> None:
        leader = read_leader()
        standby = other(leader)
        try:
            with connect(leader) as conn, conn.cursor() as cur:
                if self.failures:
                    log(f"primaire {leader} de nouveau joignable après {self.failures} échec(s)")
                self.failures, self.first_failure_at = 0, None
                self.manage_replication(cur, standby)
            # isolement : à chaque tour juste après une bascule, puis toutes les 30 s
            self.ticks += 1
            if time.time() < self.watch_old_leader_until or self.ticks % 30 == 0:
                self.fence(standby)
            if self.cdc_realign_pending:
                self.cdc_realign_pending = not realign_cdc_on_slot()
            elif self.ticks % 5 == 0:
                restart_failed_cdc_tasks()
        except psycopg2.OperationalError:
            self.failures += 1
            if self.failures == 1:
                self.first_failure_at = time.time()
            log(f"primaire {leader} injoignable ({self.failures}/{FAILURES_TO_FAILOVER})")
            if self.failures >= FAILURES_TO_FAILOVER:
                self.failover(standby)


def main() -> None:
    log(f"surveillance de {', '.join(NODES)} ; primaire actuel : {read_leader()}")
    watchdog = Watchdog()
    while True:
        try:
            watchdog.tick()
        except Exception as exc:                  # noqa: BLE001 — le gardien ne doit jamais s'arrêter
            log(f"erreur inattendue : {exc}")
        time.sleep(CHECK_EVERY_S)


if __name__ == "__main__":
    main()
