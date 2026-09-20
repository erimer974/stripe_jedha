# Maquette : version réduite et exécutable de l'architecture cible

Cette maquette **démontre le fonctionnement du plan cible**
([`00_plan_pipeline_cible.svg`](../05_pipeline/00_plan_pipeline_cible.svg)) sur un seul poste, avec
Docker Compose. Elle n'en est pas une alternative : elle reprend **les mêmes étapes, dans le même
ordre, avec les mêmes couches, tables, rôles et DAG**. Les composants qui ne peuvent pas tourner en
local sont remplacés par un composant jouant **le même rôle, au même endroit du plan** (§2).

Plans : [`10_plan_pipeline_maquette.svg`](../05_pipeline/10_plan_pipeline_maquette.svg) ·
comparaison boîte par boîte :
[`20_plan_pipeline_cible_et_maquette.svg`](../05_pipeline/20_plan_pipeline_cible_et_maquette.svg).

---

## 1. Cible → maquette

| Boîte du plan | Cible | Maquette |
|---|---|---|
| **SOURCES** : OLTP | PostgreSQL | `oltp-db`, PostgreSQL 16, modèle [`02`](../02_modele_oltp/schema_oltp.sql) — identique |
| **SOURCES** : événements | application Stripe | `app-simulator` : écrit dans l'OLTP et produit des fichiers JSON Lines |
| **SOURCES** : référentiels | pays, taux de change | `sources/reference/*.csv` |
| **INGESTION (EL)** | Airbyte | tâches `el_sync.*` (`pipeline/el_sync.py`) : mêmes modes (incrémental dédupliqué sur `created_at`, complet, état des flux) |
| **STOCKAGE** : RAW | Snowflake RAW | `warehouse`, schéma `raw`, mêmes tables |
| **STOCKAGE** : MongoDB | MongoDB | `mongo`, MongoDB 7, sous-ensemble du modèle [`04`](../04_modele_nosql/collections_mongodb.js) |
| **TRANSFORMATION (T)** | dbt | dbt 1.8 : `dbt run`, `dbt source freshness`, `dbt test` — identique |
| **ANALYTICS** | étoile + agrégats | schéma `analytics` : `fact_transactions`, 4 dimensions, `agg_revenue_daily` |
| **Modèle fraude** | MLflow, scoring, write-back | serveur MLflow, alias `champion`, scores dans `ml.fraud_scores` et dans l'OLTP — identique |
| Dérive | Evidently | PSI calculé dans `pipeline/ml_fraud.py`, suivi dans MLflow |
| **BI** | outil de BI | requêtes SQL (rôle `data_analyst`, [`08`](../08_requetes/README.md)) |
| **Orchestration** | Airflow | Airflow 2.9 (standalone) — identique |
| **Sécurité & conformité** | [`06`](../06_securite_conformite/README.md) | rôles au moindre privilège jusqu'à la colonne, journal d'audit, pseudonymisation, DAG quotidien (sauf TLS, §2) |

**Ordre du DAG principal, identique au plan** : `el_sync` → `dbt_transform` → `dbt_test_quality` →
`fraud_scoring`, toutes les 5 minutes. Le scoring ne tourne que si les tests passent.

## 2. Ce qui n'est pas reproduit

| Élément | Pourquoi | À la place |
|---|---|---|
| Snowflake et sa montée en charge | service cloud payant | mêmes couches et tables dans PostgreSQL |
| Airbyte | plateforme lourde (une dizaine de conteneurs) | connecteur Python, mêmes modes |
| TLS en transit | certificats nécessaires | contrôle **non bloquant**, **en échec assumé** dans le rapport |
| Chiffrement au repos, sauvegardes, reprise | infrastructure dédiée | décrits dans [`06`](../06_securite_conformite/README.md) |
| Réplication et bascule, partitionnement, replica set et sharding | plusieurs nœuds | une instance de chaque |
| `dispute_evidence`, `customer_profiles` | hors périmètre de la démonstration | décrites dans [`04`](../04_modele_nosql/README.md) |
| Personnalisation, prédictif, revue humaine | hors périmètre | décrits dans [`07`](../07_integration_ml/README.md) |
| Cloud, déploiement continu | démonstration locale | décrits dans [`05` §11](../05_pipeline/README.md) |
| Un secret par rôle dans un coffre | simplification | mot de passe commun lu dans `.env` (jamais versionné) |

## 3. Les trois DAG

| DAG | Fréquence | Tâches |
|---|---|---|
| `stripe_pipeline` | 5 min | `el_sync.oltp_postgres_to_raw`, `el_sync.reference_files_to_raw`, `el_sync.app_events_to_mongodb` → `dbt_transform` → `dbt_test_quality` → `fraud_scoring` |
| `stripe_model_retraining` | quotidien | `check_drift` → `train_candidate` → `promote_if_better` (ou `no_retraining_needed`) |
| `stripe_compliance_daily` | quotidien | `run_compliance_checks` → `assert_blocking_controls` |

## 4. Lancement

Prérequis : Docker Desktop (8 Go au moins), Git Bash. Arrêter d'abord les autres projets Compose :

```bash
docker compose ls
```

Puis, depuis ce dossier :

```bash
cp .env.example .env
```

```bash
docker compose up -d --build
```

Environ 1 minute de démarrage (5 minutes de construction au premier lancement). Vérifier :

```bash
docker compose ps
```

| Interface | Adresse | Identifiants |
|---|---|---|
| Airflow | http://localhost:8082 | `airflow` / `airflow` (valeurs `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` du `.env`) |
| MLflow | http://localhost:5001 | — |
| OLTP (DBeaver) | `localhost:5435`, base `stripe_oltp` | par exemple `data_analyst` / `SERVICE_PASSWORD` |
| Entrepôt (DBeaver) | `localhost:5436`, base `warehouse` | `data_analyst` / `SERVICE_PASSWORD` |
| MongoDB (Compass) | `mongodb://ml_reader:<SERVICE_PASSWORD>@localhost:27018/stripe_nosql?authSource=stripe_nosql` | — |

## 5. Préparer une démonstration (≈ 15 min)

Les DAG sont en pause au premier démarrage.

1. **Activer `stripe_pipeline`** et le laisser tourner 10 minutes (le simulateur produit 40
   transactions/minute). Le scoring est « skipped » tant qu'aucun modèle n'existe : c'est normal.
2. **Activer puis lancer `stripe_model_retraining`**. Sans modèle, `check_drift` décide
   `bootstrap` : un premier modèle est entraîné et promu `champion`. Sous 200 transactions
   étiquetées, l'entraînement est reporté — attendre quelques minutes.
3. **Activer `stripe_compliance_daily`** : il produit le premier rapport immédiatement.
4. *(Facultatif, pour montrer la dérive)* injecter un lot modifié, puis relancer les deux DAG :

   ```bash
   MSYS_NO_PATHCONV=1 docker compose exec app-simulator python /opt/app/app_simulator.py --once --n 600 --drift
   ```

   (`MSYS_NO_PATHCONV=1` évite que Git Bash réécrive le chemin ; inutile sous PowerShell.)
   Résultat observé : PSI de 0,44 sur `amt`, 3,39 sur `geo_distance_km`, 0,34 sur `velocity_1h`
   (seuil 0,2) → `drift` ; le candidat v2 (F1 0,97) remplace le champion v1 (F1 0,34).

## 6. Requêtes et démonstrations

Depuis ce dossier, dans Git Bash.

| Fichier | Ce qu'il montre | Commande |
|---|---|---|
| `queries/01_bi_analytics.sql` | Q1 à Q13 : revenus, série temporelle, fraude, segmentation | `source .env; docker compose exec -T warehouse psql "postgresql://data_analyst:${SERVICE_PASSWORD}@localhost/warehouse" < queries/01_bi_analytics.sql` |
| `queries/02_oltp_operations.sql` | Q6 à Q8 : opérationnel, score réécrit, vue pseudonymisée | `source .env; docker compose exec -T oltp-db psql "postgresql://data_analyst:${SERVICE_PASSWORD}@localhost/stripe_oltp" < queries/02_oltp_operations.sql` |
| `queries/03_mongodb.js` | N1 à N7 : features, sessions, satisfaction, journaux | `source .env; MSYS_NO_PATHCONV=1 docker compose exec mongo mongosh "mongodb://ml_reader:${SERVICE_PASSWORD}@localhost/stripe_nosql?authSource=stripe_nosql" --quiet --file /queries/03_mongodb.js` |
| `queries/04_reconciliation_transaction_id.sh` | **une même transaction** dans l'OLTP, l'étoile et MongoDB | `bash queries/04_reconciliation_transaction_id.sh` |
| `queries/05_security_demo.sh` | refus d'accès attendus et dernier rapport de conformité | `bash queries/05_security_demo.sh` |

Les fichiers `01` à `03` sont des copies exécutables du livrable 8.

## 7. Arborescence

```
maquette/
├── docker-compose.yml        6 services : oltp-db, app-simulator, warehouse, mongo, mlflow, airflow
├── Dockerfile                image commune (Airflow + dbt + MLflow + pymongo + scikit-learn)
├── .env.example              mots de passe (copier en .env, jamais versionné)
├── sources/                  application simulée + données de référence (CSV)
├── init/                     schémas, rôles, RGPD, couches de l'entrepôt, utilisateurs et index Mongo
├── pipeline/                 el_sync.py (ingestion), ml_fraud.py (dérive, entraînement, scoring), compliance.py
├── dags/                     les 3 DAG Airflow
├── dbt_stripe/               transformation : staging, étoile, tests
└── queries/                  BI, OLTP, MongoDB, réconciliation, sécurité
```

## 8. Arrêt

```bash
docker compose stop
```

`stop` garde les données. Pour tout effacer :

```bash
docker compose down -v
```
