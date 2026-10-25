# Maquette : version réduite et exécutable de l'architecture cible

Cette maquette **démontre le fonctionnement du plan cible** ([`05_pipeline`](../05_pipeline/README.md))
sur un seul poste, avec Docker Compose. Elle n'en est pas une alternative : elle reprend **les
trois chemins (synchrone, flux, lots), dans le même ordre, avec les mêmes couches, topics, tables,
rôles et DAG**. Les composants qui ne peuvent pas tourner en local sont remplacés par un composant
jouant **le même rôle, au même endroit du plan** (§2).

---

## 1. Cible → maquette

| Boîte du plan | Cible | Maquette |
|---|---|---|
| **SOURCES** : OLTP | PostgreSQL, 3 nœuds, Patroni, HAProxy | `oltp-db` + `oltp-replica` (PostgreSQL 17, réplica synchrone), gardien `failover-watchdog` (rôle de Patroni), routeur `oltp-router` (rôle d'HAProxy), modèle [`02`](../02_modele_oltp/schema_oltp.sql), journal en réplication logique |
| **SOURCES** : API de paiement, applications | application Stripe | `app-simulator` : appelle le scoring, écrit dans l'OLTP, publie dans Kafka |
| **SOURCES** : référentiels | pays, taux de change | `sources/reference/*.csv` |
| **CHEMIN SYNCHRONE** : scoring | service de scoring | `scoring-api` (`pipeline/scoring_api.py`) : champion MLflow en mémoire, rechargé à la promotion |
| **VOIE FLUX** : capture des changements | Debezium | `debezium` : **Debezium 2.7** — identique ; connecteur [`cdc/debezium-oltp.json`](cdc/debezium-oltp.json) |
| **VOIE FLUX** : bus d'événements | Kafka | `redpanda` : même protocole que Kafka, mêmes topics, rétention 7 jours |
| **VOIE FLUX** : chargement continu | Snowpipe Streaming, connecteur MongoDB | `stream-processor` (`pipeline/stream_processor.py`) : offset validé après écriture, upsert |
| **VOIE LOTS** : référentiels | Airbyte | tâche `el_sync.reference_files_to_raw` (`pipeline/el_sync.py`), rechargement complet |
| **STOCKAGE** : RAW | Snowflake RAW | `warehouse`, schéma `raw`, mêmes tables |
| **STOCKAGE** : MongoDB | MongoDB | `mongo`, MongoDB 7, sous-ensemble du modèle [`04`](../04_modele_nosql/collections_mongodb.js) |
| **TRANSFORMATION (T)** | dbt | dbt 1.8 : `dbt run`, `dbt source freshness`, `dbt test` — identique |
| **ANALYTICS** | étoile + agrégats | schéma `analytics` : `fact_transactions`, 4 dimensions, `agg_revenue_daily` |
| **Modèle fraude** | MLflow, scoring en ligne et rattrapage | serveur MLflow, alias `champion`, scores dans l'OLTP, `fraud.scores` et `ml.fraud_scores` — identique |
| Dérive | Evidently | PSI calculé dans `pipeline/ml_fraud.py`, suivi dans MLflow |
| **BI** | outil de BI | requêtes SQL (rôle `data_analyst`, [`08`](../08_requetes/README.md)) |
| **Orchestration** | Airflow | Airflow 2.9 (standalone) — identique |
| **Sécurité & conformité** | [`06`](../06_securite_conformite/README.md) | rôles au moindre privilège jusqu'à la colonne (dont `cdc_reader`), email exclu de la publication CDC, journal d'audit, pseudonymisation, DAG quotidien (sauf TLS, §2) |

**Le chemin d'une transaction, identique au plan :**

1. `app-simulator` envoie les features à `scoring-api` → score et décision en quelques ms ;
2. il enregistre la transaction **avec son score** dans `oltp-db`, puis publie features, score et
   événements dans Kafka ;
3. `debezium` lit la transaction dans le journal de `oltp-db` et la publie dans
   `cdc.oltp.transactions` ;
4. `stream-processor` écrit la transaction dans `raw`, les features dans MongoDB, le score dans
   `ml.fraud_scores` ;
5. toutes les 5 minutes, le DAG `stripe_pipeline` construit et teste l'étoile, puis score les
   éventuelles transactions restées sans score.

## 2. Ce qui n'est pas reproduit

| Élément | Pourquoi | À la place |
|---|---|---|
| Kafka à 6 brokers sur 3 zones | plusieurs machines | un nœud Redpanda, réplication 1 |
| Registre de schémas (Avro) | simplification | messages JSON |
| Snowflake et sa montée en charge | service cloud payant | mêmes couches et tables dans PostgreSQL |
| Snowpipe Streaming, connecteur MongoDB | connecteurs propres au cloud | consommateurs Python, même garantie de livraison |
| Airbyte | plateforme lourde (une dizaine de conteneurs) | tâche Python, même mode |
| Profil client en ligne (`customer_profiles`) | hors périmètre | le simulateur fournit lui-même la vélocité |
| TLS en transit | certificats nécessaires | contrôle **non bloquant**, **en échec assumé** dans le rapport |
| Chiffrement au repos, sauvegardes, reprise | infrastructure dédiée | décrits dans [`06`](../06_securite_conformite/README.md) |
| 3 zones, quorum de 2 réplicas, Patroni et etcd | plusieurs serveurs | 2 nœuds OLTP, gardien et routeur en Python : réplication synchrone, bascule et isolement **reproduits** (§6 ter) |
| Replica set et sharding MongoDB, sharding OLTP | plusieurs serveurs | une instance ; partitionnement mensuel de l'OLTP **reproduit** |
| Région de secours, sauvegardes | infrastructure dédiée | décrites dans [`05` §9](../05_pipeline/README.md) |
| `dispute_evidence`, stockage objet | hors périmètre de la démonstration | décrits dans [`04`](../04_modele_nosql/README.md) |
| Personnalisation, prédictif, revue humaine | hors périmètre | décrits dans [`07`](../07_integration_ml/README.md) |
| Cloud, déploiement continu | démonstration locale | décrits dans [`05` §12](../05_pipeline/README.md) |
| Un secret par rôle dans un coffre | simplification | mot de passe commun lu dans `.env` (jamais versionné) |

## 3. Les quatre DAG (voie lots)

| DAG | Fréquence | Tâches |
|---|---|---|
| `stripe_pipeline` | 5 min | `el_sync.reference_files_to_raw` → `dbt_transform` → `dbt_test_quality` → `fraud_scoring_catch_up` |
| `stripe_model_retraining` | quotidien | `check_drift` → `train_candidate` → `promote_if_better` (ou `no_retraining_needed`) |
| `stripe_compliance_daily` | quotidien | `run_compliance_checks` → `assert_blocking_controls` |
| `stripe_capacity` | toutes les heures | `check_capacity` : partitions à venir, débit de pointe / capacité (`CAPACITY_TPS`), retard Kafka → OK, VIGILANCE ou AGRANDIR (échec) |

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

Environ 1 minute de démarrage (5 minutes de construction au premier lancement). Vérifier que les
14 services tournent (`kafka-init` et `cdc-init` s'arrêtent normalement après leur travail) :

```bash
docker compose ps -a
```

| Interface | Adresse | Identifiants |
|---|---|---|
| Airflow | http://localhost:8082 | `airflow` / `airflow` (valeurs `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` du `.env`) |
| MLflow | http://localhost:5001 | — |
| Console Kafka (Redpanda Console) | http://localhost:8085 | — |
| Service de scoring | http://localhost:8086/health (ou 8087, 8088 : le premier port libre ; `docker compose port scoring-api 8000` l'affiche) | — |
| OLTP, nœud réplica (DBeaver) | `localhost:5437`, base `stripe_oltp` | lecture seule |
| Debezium (Kafka Connect) | http://localhost:8083/connectors/oltp-cdc/status | — |
| OLTP (DBeaver) | `localhost:5435`, base `stripe_oltp` | par exemple `data_analyst` / `SERVICE_PASSWORD` |
| Entrepôt (DBeaver) | `localhost:5436`, base `warehouse` | `data_analyst` / `SERVICE_PASSWORD` |
| MongoDB (Compass) | `mongodb://ml_reader:<SERVICE_PASSWORD>@localhost:27018/stripe_nosql?authSource=stripe_nosql` | — |

## 5. Préparer une démonstration (≈ 15 min)

Les DAG sont en pause au premier démarrage.

1. **Activer `stripe_pipeline`** et le laisser tourner 5 minutes (le simulateur produit 40
   transactions par minute). Tant qu'aucun modèle n'existe, l'API de paiement applique sa règle
   de repli (`fallback` dans le journal du simulateur) et le rattrapage est « skipped » : c'est
   normal.
2. **Activer puis lancer `stripe_model_retraining`**. Sans modèle, `check_drift` décide
   `bootstrap` : un premier modèle est entraîné et promu `champion`. Sous 200 transactions
   étiquetées, l'entraînement est reporté — attendre quelques minutes.
3. Dans les 30 secondes, `scoring-api` charge le champion. Le journal du simulateur affiche
   alors les décisions en ligne et la latence :

   ```bash
   docker compose logs -f app-simulator
   ```

   Résultat observé : `décisions {'accept': 20, ...} ; scoring p50 8 ms, p99 12 ms`. Au passage
   suivant de `stripe_pipeline`, le rattrapage score les transactions passées par la règle de
   repli (observé : 220 scorées, 220 réécrites dans l'OLTP).
4. **Activer `stripe_compliance_daily`** : il produit le premier rapport immédiatement.
5. *(Facultatif, pour montrer la dérive)* injecter un lot modifié, puis relancer
   `stripe_model_retraining` :

   ```bash
   MSYS_NO_PATHCONV=1 docker compose exec app-simulator python /opt/app/app_simulator.py --once --n 600 --drift
   ```

   (`MSYS_NO_PATHCONV=1` évite que Git Bash réécrive le chemin ; inutile sous PowerShell.)

## 6. Démonstrations de la voie flux

**Voir les messages passer** : Redpanda Console → *Topics* → `cdc.oltp.transactions`. Ou en ligne
de commande :

```bash
docker compose exec redpanda rpk topic consume cdc.oltp.transactions -n 3
```

**L'email ne quitte pas l'OLTP** : les messages de `cdc.oltp.customers` ne contiennent que
`customer_id`, `country_code` et `created_at` (observé : 0 message sur 31 contenant un email).

**Panne d'un consommateur, sans perte ni doublon** :

```bash
docker compose stop stream-processor
```

Attendre une minute (les transactions s'accumulent dans Kafka, l'OLTP continue d'enregistrer),
puis :

```bash
docker compose start stream-processor
```

Résultat observé : pendant la panne, 420 transactions dans l'OLTP et 380 dans `raw` ; 15 secondes
après le redémarrage, 440 et 440, sans doublon.

**Panne du service de scoring** : `docker compose stop scoring-api` → après 3 échecs, le
**disjoncteur** de l'API de paiement s'ouvre : les paiements continuent par la règle de repli
(`fallback`) sans attendre le service. `docker compose start scoring-api` → le circuit se referme
au premier appel réussi, et au passage suivant le rattrapage score les transactions restées sans
score. Résultat observé : latence médiane de 0 ms pendant la panne ; un paiement d'essai toutes
les 30 secondes attend 4 s (le nom d'un conteneur arrêté met ce temps à ne pas se résoudre ; en
cible, le service est derrière une adresse stable de répartiteur de charge).

## 6 bis. Démonstrations de montée en charge

**Test de charge de bout en bout** : plusieurs API de paiement en parallèle, scoring compris ;
mesure du débit, du temps de rattrapage de la voie flux et du retard Kafka :

```bash
bash charge/test_de_charge.sh 12 500
```

**Ajouter des exemplaires du service de scoring** (le goulot de la maquette), puis relancer :

```bash
docker compose up -d --scale scoring-api=3 --no-recreate scoring-api
```

```bash
MSYS_NO_PATHCONV=1 docker compose exec app-simulator python /opt/charge/charge_scoring.py --requests 3000 --clients 24
```

**Ajouter des consommateurs Kafka** : les partitions se répartissent entre eux.

```bash
docker compose up -d --scale stream-processor=3 --no-recreate stream-processor
```

```bash
docker compose exec redpanda rpk group describe sink-raw
```

Revenir à un exemplaire : `docker compose up -d --scale scoring-api=1 --scale stream-processor=1 --no-recreate`.

Résultats observés :

| Test | Résultat |
|---|---|
| scoring, 48 clients, 1 exemplaire → 3 exemplaires | 378 → **697 req/s** ; médiane 117 → **44 ms** |
| chaîne complète 12 × 500, 1 exemplaire du scoring | **288 tx/s** (plafond : le scoring sature) ; RAW à jour 1,3 s après |
| chaîne complète 12 × 500, 3 exemplaires | 392 tx/s ; 16 × 1 000 : **460 tx/s** ; RAW à jour 1,4 s après |
| surveillance de capacité (`CAPACITY_TPS=290`, le plafond mesuré) | 49 % → OK ; après une charge plus forte, 92 % → **AGRANDIR** (tâche en échec) |
| 3 consommateurs Kafka | une partition chacun, retard nul |

## 6 ter. Démonstrations de continuité (cluster OLTP)

Le cluster OLTP a deux nœuds PostgreSQL 17 : `oltp-db` (primaire au départ) et `oltp-replica`
(réplica **synchrone** : chaque paiement validé existe sur les deux). Le **gardien**
(`failover-watchdog`, rôle de Patroni) surveille le primaire chaque seconde ; le **routeur**
(`oltp-router`, rôle d'HAProxy) donne à tous les clients une seule adresse, toujours vers le
primaire du moment.

**Test complet de bascule** : une sonde enregistre des paiements un par un pendant 70 s ; au bout
de 15 s, le primaire est arrêté brutalement ; le script vérifie ensuite le RPO (paiements confirmés
perdus), le RTO (interruption des écritures) et la couche raw :

```bash
bash ha/test_bascule.sh
```

Réintégrer l'ancien primaire (il se reconstruit tout seul en réplica, la réplication redevient
synchrone) :

```bash
docker compose start oltp-db
```

Suivre le gardien et le routeur :

```bash
docker compose logs -f failover-watchdog oltp-router
```

**Isolement d'un ancien primaire qui se réveille** : geler le primaire, attendre la bascule, puis
le réveiller ; le gardien le passe aussitôt en lecture seule. Le redémarrer pour qu'il redevienne
réplica :

```bash
docker compose pause oltp-db
```

```bash
docker compose unpause oltp-db
```

```bash
docker compose restart oltp-db
```

(Remplacer `oltp-db` par le nom du primaire du moment ; il est écrit dans les journaux du gardien.)

Résultats observés :

| Test | Résultat |
|---|---|
| panne brutale du primaire | bascule en **8,9 s** (détection 6 s, promotion 3 s) ; interruption vue par l'API **9,2 s** |
| RPO | **3 052 paiements confirmés, 0 absent** du nouveau primaire |
| capture des changements | Debezium relancé et réaligné sur le slot recopié ; raw = OLTP (3 152), **0 doublon** |
| panne du réplica | écritures suspendues 7,6 s, puis passage en asynchrone (alerte « mode dégradé ») |
| ancien primaire gelé puis réveillé | isolé en lecture seule en 1 s ; redémarré, reconstruit en réplica |

Défaut trouvé pendant ces tests et corrigé : sans réalignement, Debezium sautait les transactions
des premières secondes après la bascule (450 transactions). Détail : livrable 5 §9.2.

## 6 quater. Démonstrations de gestion des erreurs

**Tests automatisés** (32 tests : messages invalides, refus de la base, référentiels, disjoncteur,
service de scoring, capacité, alertes) :

```bash
MSYS_NO_PATHCONV=1 docker compose exec airflow python -m pytest -q /opt/airflow/tests -p no:cacheprovider
```

**Données invalides injectées en marche** : trois messages cassés publiés dans Kafka et une
transaction au client inconnu ; ils partent en quarantaine avec leur raison, le flux continue, des
alertes sont enregistrées :

```bash
bash queries/08_injection_erreurs.sh
```

**Consulter la quarantaine et les alertes** :

```bash
source .env; docker compose exec warehouse psql -U "$PG_ADMIN_USER" -d warehouse -c "SELECT source, location, error FROM raw._quarantine ORDER BY 1 DESC LIMIT 10" -c "SELECT raised_at, severity, source, message FROM audit.alerts ORDER BY 1 DESC LIMIT 10"
```

**Panne de l'entrepôt** : `docker compose stop warehouse`, attendre une minute, puis
`docker compose start warehouse` ; le journal du consommateur montre les nouveaux essais
(1, 2, 4, 8, 16, 30 s), l'alerte CRITICAL et l'alerte « rétabli ». L'alerte CRITICAL n'est écrite que
dans le journal : la table `audit.alerts` est dans l'entrepôt arrêté (en cible, l'outil d'astreinte la reçoit).

**Airflow pendant la panne de l'entrepôt** : les métadonnées d'Airflow sont sur la même instance
PostgreSQL que l'entrepôt. Quand elle s'arrête, le planificateur d'Airflow s'arrête aussi. Un contrôle de
santé le surveille toutes les 20 s : après trois contrôles manqués, le conteneur `airflow` s'arrête en
erreur et Docker le redémarre (`restart: unless-stopped`), en réessayant tant que l'entrepôt est absent.
Au retour de l'entrepôt, Airflow reprend seul et les DAG repartent au passage suivant. Pour suivre la
reprise :

```bash
docker compose ps airflow
```

```bash
docker compose logs -f airflow | grep "\[airflow\]"
```

Si `docker compose ps` affiche `unhealthy` plus de trois minutes après le retour de l'entrepôt, relancer
à la main : `docker compose restart airflow`.

Résultats observés :

| Test | Résultat |
|---|---|
| 3 messages invalides | 3 rejets en quarantaine, raison exacte, flux non interrompu, 3 alertes |
| client inconnu | isolé par dbt (`analytics.quarantine_transactions`), tests dbt au vert |
| fichier de taux avec 3 lignes fausses sur 4 | fichier refusé, anciens taux conservés, 3 lignes en quarantaine, alerte CRITICAL |
| entrepôt arrêté 75 s | rattrapage complet (OLTP 1 000 = raw 1 000), alertes CRITICAL puis INFO ; Airflow redémarré seul (1 fois), sain 30 s après le retour de l’entrepôt |
| tests automatisés | 32 sur 32 |

## 7. Requêtes et démonstrations

Depuis ce dossier, dans Git Bash.

| Fichier | Ce qu'il montre | Commande |
|---|---|---|
| `queries/01_bi_analytics.sql` | Q1 à Q13 : revenus, série temporelle, fraude, segmentation | `source .env; docker compose exec -T warehouse psql "postgresql://data_analyst:${SERVICE_PASSWORD}@localhost/warehouse" < queries/01_bi_analytics.sql` |
| `queries/02_oltp_operations.sql` | Q6 à Q8 : opérationnel, score enregistré, vue pseudonymisée | `source .env; docker compose exec -T oltp-db psql "postgresql://data_analyst:${SERVICE_PASSWORD}@localhost/stripe_oltp" < queries/02_oltp_operations.sql` |
| `queries/03_mongodb.js` | N1 à N7 : features, sessions, satisfaction, journaux | `source .env; MSYS_NO_PATHCONV=1 docker compose exec mongo mongosh "mongodb://ml_reader:${SERVICE_PASSWORD}@localhost/stripe_nosql?authSource=stripe_nosql" --quiet --file /queries/03_mongodb.js` |
| `queries/04_reconciliation_transaction_id.sh` | **une même transaction** dans l'OLTP, l'étoile et MongoDB | `bash queries/04_reconciliation_transaction_id.sh` |
| `queries/05_security_demo.sh` | refus d'accès attendus et dernier rapport de conformité | `bash queries/05_security_demo.sh` |
| `queries/06_evolution_schema.sh` | une colonne ajoutée en marche : publiée dans Kafka, aucune erreur ; celle des clients n'est pas publiée | `bash queries/06_evolution_schema.sh` puis `bash queries/06_evolution_schema.sh --annuler` |
| `queries/07_partitionnement.sql` | partitions mensuelles ; une requête sur un mois ne lit qu'une partition | `source .env; docker compose exec -T oltp-db psql "postgresql://data_analyst:${SERVICE_PASSWORD}@localhost/stripe_oltp" < queries/07_partitionnement.sql` |
| `queries/08_injection_erreurs.sh` | données invalides : quarantaine, flux non interrompu, alertes | `bash queries/08_injection_erreurs.sh` |

Les fichiers `01` à `03` sont des copies exécutables du livrable 8.

## 8. Arborescence

```
maquette/
├── docker-compose.yml        16 services : sources, cluster OLTP, chemin synchrone, voie flux, stockage, valorisation, orchestration
├── Dockerfile                image commune (Airflow + dbt + MLflow + pymongo + scikit-learn + client Kafka)
├── .env.example              mots de passe (copier en .env, jamais versionné)
├── sources/                  application simulée + données de référence (CSV)
├── cdc/                      configuration du connecteur Debezium
├── ha/                       cluster OLTP : démarrage des nœuds, gardien, routeur, sonde et test de bascule
├── charge/                   tests de charge : chaîne complète, service de scoring
├── init/                     schémas, rôles, publication CDC, RGPD, couches de l'entrepôt, utilisateurs et index Mongo
├── pipeline/                 scoring_api.py (scoring en ligne), stream_processor.py (consommateurs Kafka),
│                             el_sync.py (référentiels), ml_fraud.py (dérive, entraînement, rattrapage), compliance.py,
│                             capacity.py (surveillance de capacité), errors.py, alerting.py,
│                             dag_defaults.py (erreurs passagères ou définitives, alertes, nouveaux essais)
├── dags/                     les 4 DAG Airflow
├── dbt_stripe/               transformation : staging, étoile, quarantaine métier, tests
├── tests/                    tests automatisés (pytest) de la gestion des erreurs
└── queries/                  BI, OLTP, MongoDB, réconciliation, sécurité, évolution de schéma,
                              partitionnement, injection d'erreurs
```

## 9. Arrêt

```bash
docker compose stop
```

`stop` garde les données. Pour tout effacer :

```bash
docker compose down -v
```
