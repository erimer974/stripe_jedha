"""
============================================================
ROUTEUR DU CLUSTER OLTP — rôle d'HAProxy dans la maquette (Livrable 5 §9)
------------------------------------------------------------
Une adresse unique, oltp-router:5432, pour tous les clients (API de paiement,
Debezium, Airflow) : chaque nouvelle connexion est envoyée vers le primaire
du moment, désigné par le fichier /ha/leader que le gardien met à jour.
Après une bascule, les connexions encore ouvertes vers l'ancien primaire
sont coupées : les clients se reconnectent, et arrivent sur le nouveau.

Usage : python /opt/ha/primary_router.py
============================================================
"""
from __future__ import annotations

import asyncio
import os
from datetime import datetime

LEADER_FILE = "/ha/leader"
PORT = 5432
CONNECT_TIMEOUT_S = 2
open_links: dict[str, set[asyncio.StreamWriter]] = {}


def log(message: str) -> None:
    print(f"[routeur {datetime.now():%H:%M:%S}] {message}", flush=True)


def read_leader() -> str:
    try:
        with open(LEADER_FILE, encoding="utf-8") as fh:
            return fh.read().strip() or os.environ["INITIAL_LEADER"]
    except FileNotFoundError:
        return os.environ["INITIAL_LEADER"]


async def pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        while data := await reader.read(65536):
            writer.write(data)
            await writer.drain()
    except (ConnectionError, OSError):
        pass
    finally:
        writer.close()


async def handle(client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter) -> None:
    leader = read_leader()
    try:
        upstream_reader, upstream_writer = await asyncio.wait_for(
            asyncio.open_connection(leader, PORT), CONNECT_TIMEOUT_S)
    except (OSError, asyncio.TimeoutError):
        client_writer.close()                     # primaire injoignable : le client réessaiera
        return
    links = open_links.setdefault(leader, set())
    links.update({client_writer, upstream_writer})
    try:
        await asyncio.gather(pipe(client_reader, upstream_writer), pipe(upstream_reader, client_writer))
    finally:
        links.discard(client_writer)
        links.discard(upstream_writer)


async def watch_leader() -> None:
    """Quand le primaire change, coupe les connexions qui visent encore l'ancien."""
    current = read_leader()
    while True:
        await asyncio.sleep(1)
        leader = read_leader()
        if leader != current:
            stale = open_links.pop(current, set())
            for writer in stale:
                writer.close()
            log(f"primaire : {current} -> {leader} ; {len(stale) // 2} connexion(s) vers l'ancien coupée(s)")
            current = leader


async def main() -> None:
    server = await asyncio.start_server(handle, "0.0.0.0", PORT)
    log(f"écoute sur le port {PORT}, primaire actuel : {read_leader()}")
    async with server:
        await asyncio.gather(server.serve_forever(), watch_leader())


if __name__ == "__main__":
    asyncio.run(main())
