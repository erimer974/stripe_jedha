# Livrable 8 : requêtes SQL et NoSQL

**Énoncé :** un ensemble de requêtes SQL et NoSQL montrant comment répondre aux questions clés de
l'entreprise (revenus, fraude, segmentation) avec les modèles proposés.

| Fichier | Système | Requêtes |
|---|---|---|
| [`requetes_olap.sql`](requetes_olap.sql) | OLAP (couche ANALYTICS) | Q1 à Q5, Q9 à Q14 |
| [`requetes_oltp.sql`](requetes_oltp.sql) | OLTP PostgreSQL | Q6 à Q8 |
| [`requetes_mongodb.js`](requetes_mongodb.js) | MongoDB | N1 à N8 |
| [`requetes_matrice.svg`](requetes_matrice.svg) | — | matrice question / domaine / système / technique |

![Matrice des requêtes](requetes_matrice.svg)

---

## 1. Chaque question au système conçu pour elle

| Nature de la question | Système | Pourquoi |
|---|---|---|
| **analytique** : historique, agrégations, tendances | OLAP | étoile dénormalisée, pré-agrégats, micro-partitions regroupées par date (clustering) |
| **opérationnelle** : dernières minutes, alerte, détail d'un client | OLTP | donnée la plus fraîche, lecture par index |
| **comportementale** : parcours, avis, journaux, features | MongoDB | données imbriquées, pipelines d'agrégation |

Poser une question analytique à l'OLTP ralentirait les paiements ; poser une question
opérationnelle à l'OLAP donnerait une réponse vieille de plusieurs minutes.

## 2. Couverture des questions métier

| Question métier | Requêtes |
|---|---|
| **Analyse des revenus** | Q1 (mensuel par commerçant), Q3 (quotidien, pré-agrégat), Q9 (par pays), Q10 (série temporelle) |
| **Détection des fraudes** | Q2 (par segment), Q4 (commerçants exposés), Q7 (file de revue), Q11 (montants), N1, N8 |
| **Segmentation de la clientèle** | Q2, Q8 (meilleurs clients pseudonymisés), Q13 (segments par pays) |
| Performance des produits | Q12 (réussite et remboursements par moyen de paiement) |
| Comportement et satisfaction | N2, N5 (parcours), N3, N6 (satisfaction) |
| Supervision et qualité | Q5 (couverture du scoring), Q6, N4, N7 |
| Conformité | Q14 (historique des contrôles, rôle `auditor`) |

## 3. Techniques mises en œuvre

Jointure faits × dimensions et agrégation (Q1, Q2, Q4, Q9, Q12, Q13) · lecture d'une table de
synthèse plutôt que des faits (Q3, Q10) · fonctions de fenêtre sur table de dates continue :
moyenne mobile 7 jours, variation (Q10) · agrégation conditionnelle `AVG(CASE …)` (Q2, Q12) · vue
pseudonymisée (Q8) · pipelines MongoDB : `$unwind` sur tableau imbriqué (N2, N5),
`$dateToString` (N6), `$addToSet` (N7), groupement sur un champ extrait d'un XML (N8).

**SQL portable** : j'ai écrit `requetes_olap.sql` en n'utilisant que des fonctions qui existent
aussi bien dans Snowflake que dans PostgreSQL. Le même fichier sert donc la cible et la maquette,
sans rien réécrire.

## 4. Exécution dans la maquette

Q1 à Q14 et N1 à N7 ont été **exécutées sans erreur**. N8 porte sur `dispute_evidence`, absente de
la maquette ; elle a été vérifiée sur une base créée avec
[`collections_mongodb.js`](../04_modele_nosql/collections_mongodb.js).

Copies exécutables dans [`maquette/queries`](../maquette/queries). Commandes depuis `maquette`,
sous Git Bash, maquette démarrée :

```bash
source .env; docker compose exec -T warehouse psql "postgresql://data_analyst:${SERVICE_PASSWORD}@localhost/warehouse" < queries/01_bi_analytics.sql
```

```bash
source .env; docker compose exec -T oltp-db psql "postgresql://data_analyst:${SERVICE_PASSWORD}@localhost/stripe_oltp" < queries/02_oltp_operations.sql
```

```bash
source .env; MSYS_NO_PATHCONV=1 docker compose exec mongo mongosh "mongodb://ml_reader:${SERVICE_PASSWORD}@localhost/stripe_nosql?authSource=stripe_nosql" --quiet --file /queries/03_mongodb.js
```

Q14, avec le rôle `auditor` :

```bash
source .env; sed -n '/^-- Q14/,$p' ../08_requetes/requetes_olap.sql | docker compose exec -T warehouse psql "postgresql://auditor:${SERVICE_PASSWORD}@localhost/warehouse"
```

**Réconciliation** :
[`queries/04_reconciliation_transaction_id.sh`](../maquette/queries/04_reconciliation_transaction_id.sh)
suit une même transaction dans l'OLTP, la table de faits et MongoDB.
