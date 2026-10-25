# Livrable 1 : diagramme d'architecture de données complet

**Énoncé :** un diagramme détaillé de l'intégration des systèmes OLTP, OLAP et NoSQL, avec les
flux de données, les pipelines et les modèles de données.

| Fichier | Contenu |
|---|---|
| [`architecture_globale.svg`](architecture_globale.svg) | l'architecture cible sur une page |

![Architecture de données globale](architecture_globale.svg)

---

## Lire le diagramme

**De gauche à droite :**

1. **Sources** : l'API de paiement écrit dans l'**OLTP PostgreSQL** ; les applications émettent des
   événements (journaux, clics, avis) ; les référentiels fournissent pays et taux de change.
2. **Chemin synchrone** : avant d'accepter un paiement, l'API appelle le **service de scoring**
   (modèle champion de MLflow) et décide en **moins de 100 ms** ; le score est enregistré avec la
   transaction.
3. **Voie flux** : **Debezium** capture chaque changement de l'OLTP dans son journal (CDC) et le
   publie dans **Kafka**, avec les événements applicatifs ; des connecteurs alimentent
   **Snowflake RAW** et **MongoDB** en **moins d'une minute**.
4. **Voie lots** : **Airbyte** charge les référentiels ; **dbt** construit le **schéma en étoile** de
   la couche ANALYTICS et ses agrégats, puis le teste ; un **scoring de rattrapage** traite les
   transactions restées sans score.
5. **Valorisation** : la BI interroge l'étoile ; **Evidently** surveille la dérive et déclenche le
   réentraînement.

**Deux bandes transversales :** **Airflow** orchestre les quatre DAG de la voie lots (toutes les
5 minutes, capacité toutes les heures, réentraînement et conformité quotidiens) ; **sécurité et conformité** s'appliquent à toutes les
étapes (TLS, chiffrement, RBAC, audit, RGPD, PCI-DSS, CCPA).

**En haut, les exigences chiffrées** (livrable 0), rangées selon les quatre questions : Quoi ?
Combien ? À quelle vitesse ? Et si ça casse ? **En bas, la synthèse** donne la réponse de
l'architecture à chacune. Au milieu, un point de la couleur de la question marque chaque élément
qui y répond : point violet (Q1) sur chaque système (une forme de donnée par système), puces cyan
(Q2) devant les annotations de volume, jaunes (Q3) devant celles de vitesse, roses (Q4) devant
celles de résistance aux pannes. Couleur des flèches : orange = chemin synchrone, violet = voie flux,
bleu = voie lots.

**Bande des modèles de données** : les tables et collections de chaque système, et la clé
`transaction_id` qui les relie.

## Les choix structurants

| Choix | Justification | Détail |
|---|---|---|
| Trois systèmes spécialisés | écrire vite, agréger vite, absorber le semi-structuré sont des optimisations incompatibles | [02](../02_modele_oltp/README.md), [03](../03_modele_olap/README.md), [04](../04_modele_nosql/README.md) |
| Montée en charge horizontale, déclenchée par la mesure | partitionnement et sharding conçus dès maintenant, activés à 70 % de la capacité mesurée chaque heure | [02 §7](../02_modele_oltp/README.md), [04 §7](../04_modele_nosql/README.md), [05 §8](../05_pipeline/README.md) |
| Clé commune `transaction_id` | relier les systèmes sans jointure physique | [05 §10](../05_pipeline/README.md) |
| ELT plutôt qu'ETL | charger le brut une fois, transformer et rejouer dans l'entrepôt | [05 §1](../05_pipeline/README.md) |
| Barrière qualité avant le rattrapage | aucune donnée non validée n'atteint les tableaux de bord ni le rattrapage | [05 §7](../05_pipeline/README.md) |
| Trois chemins : synchrone, flux, lots | chaque délai exigé par le livrable 0 tenu par l'outil le plus simple qui le permet | [05 §2](../05_pipeline/README.md) |
| Capture des changements (Debezium) et Kafka | temps réel sans charger l'OLTP ; rejeu possible sur 7 jours | [05 §6](../05_pipeline/README.md) |
| Pas de point unique de défaillance ; bascule automatique testée | RPO 0 et RTO < 30 s sur la chaîne de paiement ; une panne retarde, elle ne fait rien perdre | [02 §6](../02_modele_oltp/README.md), [05 §9](../05_pipeline/README.md) |
| Sécurité contrôlée chaque jour | la conformité se prouve, elle ne se déclare pas | [06](../06_securite_conformite/README.md) |
| Modèle explicable et réentraîné sur dérive | décision qui touche un client ; la fraude évolue | [07](../07_integration_ml/README.md) |

## Vues complémentaires

| Question | Plan |
|---|---|
| Comment circule la donnée ? | [`00_plan_pipeline_cible.svg`](../05_pipeline/00_plan_pipeline_cible.svg) |
| Où vit chaque table, qui la déplace ? | [`12_flux_donnees_par_couche.svg`](../05_pipeline/12_flux_donnees_par_couche.svg) |
| Comment est-ce déployé et surveillé ? | [`13_deploiement_supervision.svg`](../05_pipeline/13_deploiement_supervision.svg) |
| Que reproduit la maquette ? | [`20_plan_pipeline_cible_et_maquette.svg`](../05_pipeline/20_plan_pipeline_cible_et_maquette.svg) |
