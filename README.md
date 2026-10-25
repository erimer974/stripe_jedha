# Architecture de données Stripe : OLTP, OLAP et NoSQL

Architecture de données unifiée pour une plateforme de paiement : un système transactionnel
(OLTP), un entrepôt analytique (OLAP), une base documents (NoSQL), reliés par un pipeline
orchestré, sécurisés de bout en bout et valorisés par un modèle de détection de fraude.

Le dépôt contient **l'architecture cible** (modèles, plans, documents techniques) et une
**maquette exécutable** qui en démontre le fonctionnement sur un poste local.

> Étude de cas du Bloc 2 de la certification RNCP 41993 *Architecte en intelligence artificielle*.
> Données synthétiques ; projet sans lien avec l'entreprise Stripe.

![Architecture de données globale](01_architecture_globale/architecture_globale.svg)

---

## Le problème

| Besoin | Réponse |
|---|---|
| enregistrer des millions de transactions sans erreur | **OLTP PostgreSQL** normalisé, ACID, répliqué |
| analyser revenus, fraude, segments, conformité | **OLAP Snowflake**, schéma en étoile et pré-agrégats |
| exploiter journaux, parcours, avis, features ML | **MongoDB**, documents validés et indexés |
| décider d'un paiement en moins de 100 ms | **service de scoring** appelé par l'API de paiement |
| faire circuler la donnée en temps réel et de façon cohérente | **Debezium → Kafka** (flux) et **dbt** orchestré par **Airflow** (lots), clé `transaction_id` |
| respecter RGPD, PCI-DSS, CCPA | sécurité transversale et rapport de conformité quotidien |
| détecter la fraude | **MLflow** + **Evidently**, entraînement continu |

## Contenu

| Dossier | Contenu |
|---|---|
| [`00_cahier_des_charges`](00_cahier_des_charges/README.md) | exigences chiffrées : volume, vitesse, types de données, disponibilité |
| [`01_architecture_globale`](01_architecture_globale/README.md) | diagramme d'architecture complet |
| [`02_modele_oltp`](02_modele_oltp/README.md) | diagramme entité-relation et modèle physique PostgreSQL |
| [`03_modele_olap`](03_modele_olap/README.md) | schéma en étoile Snowflake, couches, performance |
| [`04_modele_nosql`](04_modele_nosql/README.md) | collections MongoDB, imbrication, index, sharding |
| [`05_pipeline`](05_pipeline/README.md) | document technique du pipeline et 6 plans |
| [`06_securite_conformite`](06_securite_conformite/README.md) | plan de sécurité et de conformité, rapport automatisé |
| [`07_integration_ml`](07_integration_ml/README.md) | stratégie d'intégration du ML |
| [`08_requetes`](08_requetes/README.md) | 22 requêtes SQL et MongoDB |
| [`maquette`](maquette/README.md) | code exécutable (Docker Compose) |

Lecture conseillée : `00` (exigences), `01` (vue d'ensemble), `05` (fonctionnement), puis `02` à `04`, `06`, `07`, `08`.

## Six principes

1. **Un système par usage** : écrire juste (OLTP), agréger vite (OLAP), absorber le semi-structuré
   (NoSQL) sont trois optimisations incompatibles.
2. **Une clé commune** : `transaction_id` relie les trois systèmes.
3. **Un chemin par délai** : décision synchrone en moins de 100 ms, voie flux (Kafka) en moins
   d'une minute, voie lots (dbt, tests, conformité) toutes les 5 minutes à quotidienne.
4. **La qualité avant la valeur** : le brut est transformé et testé dans l'entrepôt ; si un test
   échoue, la voie lots s'arrête avant le rattrapage du modèle.
5. **La sécurité se vérifie** : rôles au moindre privilège, neuf contrôles de conformité par jour.
6. **Un modèle maintenu** : la dérive déclenche un réentraînement, la promotion est conditionnelle.

## La maquette

La [maquette](maquette/README.md) reproduit la cible **à l'identique dans ses étapes, son ordre,
ses trois chemins, ses couches, ses topics, ses tables, ses rôles et ses quatre DAG**, sur seize
conteneurs Docker. Les composants
qui ne peuvent pas tourner en local sont remplacés par un composant de même rôle :

| Cible | Maquette |
|---|---|
| Kafka | Redpanda, même protocole que Kafka |
| Debezium | Debezium (identique) |
| Snowpipe Streaming, connecteur MongoDB | consommateurs Python, même garantie de livraison |
| Snowflake | PostgreSQL, mêmes couches et mêmes tables |
| Patroni, HAProxy (cluster OLTP) | gardien et routeur en Python sur 2 nœuds PostgreSQL 17 : bascule testée, RPO 0 |
| Airbyte | tâche Python pour les référentiels |
| Evidently | indice de stabilité (PSI) suivi dans MLflow |
| outil de BI | requêtes SQL |

Non reproduits : TLS et chiffrement au repos, plusieurs brokers et zones, partitionnement et
sharding, montée en charge, déploiement continu. Comparaison boîte par boîte :
[`20_plan_pipeline_cible_et_maquette.svg`](05_pipeline/20_plan_pipeline_cible_et_maquette.svg).

**Démarrage** (Docker Desktop, Git Bash) :

```bash
cd maquette
```

```bash
cp .env.example .env
```

```bash
docker compose up -d --build
```

Airflow sur http://localhost:8082, MLflow sur http://localhost:5001, console Kafka sur
http://localhost:8085.

## Limites

- **Double écriture** : l'API écrit dans l'OLTP puis publie dans Kafka ; une panne entre les deux
  perd des événements applicatifs, pas la transaction ni son score ([`05_pipeline` §10.4](05_pipeline/README.md)).
- **Messages JSON** dans la maquette ; la cible impose un registre de schémas (Avro).
- **Maquette** : données synthétiques volontairement séparables ; les performances du modèle
  illustrent le mécanisme de promotion, pas une performance réelle.

## Technologies

PostgreSQL · Debezium · Apache Kafka (Redpanda en maquette) · Snowflake · MongoDB · Airbyte ·
dbt · Apache Airflow · MLflow · Evidently · scikit-learn · Flask · pytest · Docker · GitHub Actions
