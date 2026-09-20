"""Paramètres de connexion, lus dans l'environnement (aucun secret dans le code).
Chaque tâche utilise sa propre connexion, avec le rôle au moindre privilège correspondant."""
from __future__ import annotations

import os


def env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Variable d'environnement manquante : {name}")
    return value


MONGO_DB = "stripe_nosql"
