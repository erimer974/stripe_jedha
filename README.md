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
| faire circuler la donnée de façon cohérente | **Airbyte → dbt** orchestrés par **Airflow**, clé `transaction_id` |
| respecter RGPD, PCI-DSS, CCPA | sécurité transversale et rapport de conformité quotidien |
| détecter la fraude | **MLflow** + **Evidently**, entraînement continu |

## Contenu

| Dossier | Contenu |
|---|---|
| [`01_architecture_globale`](01_architecture_globale/README.md) | diagramme d'architecture complet |
| [`02_modele_oltp`](02_modele_oltp/README.md) | diagramme entité-relation et modèle physique PostgreSQL |
| [`03_modele_olap`](03_modele_olap/README.md) | schéma en étoile Snowflake, couches, performance |
| [`04_modele_nosql`](04_modele_nosql/README.md) | collections MongoDB, imbrication, index, sharding |
| [`05_pipeline`](05_pipeline/README.md) | document technique du pipeline et 6 plans |
| [`06_securite_conformite`](06_securite_conformite/README.md) | plan de sécurité et de conformité, rapport automatisé |
| [`07_integration_ml`](07_integration_ml/README.md) | stratégie d'intégration du ML |
| [`08_requetes`](08_requetes/README.md) | 22 requêtes SQL et MongoDB |
| [`maquette`](maquette/README.md) | code exécutable (Docker Compose) |

Lecture conseillée : `01` (vue d'ensemble), `05` (fonctionnement), puis `02` à `04`, `06`, `07`, `08`.

## Six principes

1. **Un système par usage** : écrire juste (OLTP), agréger vite (OLAP), absorber le semi-structuré
   (NoSQL) sont trois optimisations incompatibles.
2. **Une clé commune** : `transaction_id` relie les trois systèmes.
3. **ELT orchestré** : le brut est chargé tel quel, puis transformé et testé dans l'entrepôt,
   toutes les 5 minutes.
4. **La qualité avant la valeur** : si un test échoue, le pipeline s'arrête avant le modèle.
5. **La sécurité se vérifie** : rôles au moindre privilège, neuf contrôles de conformité par jour.
6. **Un modèle maintenu** : la dérive déclenche un réentraînement, la promotion est conditionnelle.

## La maquette

La [maquette](maquette/README.md) reproduit la cible **à l'identique dans ses étapes, son ordre,
ses couches, ses tables, ses rôles et ses trois DAG**, sur six conteneurs Docker. Les composants
qui ne peuvent pas tourner en local sont remplacés par un composant de même rôle :

| Cible | Maquette |
|---|---|
| Snowflake | PostgreSQL, mêmes couches et mêmes tables |
| Airbyte | connecteur Python, mêmes modes de synchronisation |
| Evidently | indice de stabilité (PSI) suivi dans MLflow |
| outil de BI | requêtes SQL |

Non reproduits : TLS et chiffrement au repos, réplication, partitionnement et sharding, montée en
charge, déploiement continu. Comparaison boîte par boîte :
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

Airflow sur http://localhost:8082, MLflow sur http://localhost:5001.

## Limites

- **Latence** : micro-batch de 5 minutes ; la voie flux continu (CDC + Kafka) est décrite comme
  évolution ([`05_pipeline` §8](05_pipeline/README.md)).
- **Extraction sur `created_at`** : adaptée à un registre en ajout seul ; les modifications
  demanderaient un curseur `updated_at` ou le CDC.
- **Maquette** : données synthétiques volontairement séparables ; les performances du modèle
  illustrent le mécanisme de promotion, pas une performance réelle.

## Technologies

PostgreSQL · Snowflake · MongoDB · Airbyte · dbt · Apache Airflow · MLflow · Evidently ·
scikit-learn · Docker · GitHub Actions
