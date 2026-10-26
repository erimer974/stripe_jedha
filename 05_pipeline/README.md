# Livrable 5 : architecture du pipeline de données (document technique)

**Énoncé :** un pipeline assurant un flux cohérent **et en temps réel** entre l'OLTP, l'OLAP et le
NoSQL, avec des composants **par lots et en flux continu** ; les outils d'orchestration ; les
mécanismes d'intégration, de synchronisation et de cohérence (CDC, cohérence à terme) ;
l'évolutivité (indexation, partitionnement, cache).

Les exigences chiffrées (volumes, débits, délais, RPO/RTO) sont fixées par le
[livrable 0](../00_cahier_des_charges/README.md) ; ce document montre comment le pipeline les tient.

> **En bref** — trois chemins, chacun pour un délai :
> 1. **chemin synchrone** : l'API de paiement appelle le service de scoring et décide **avant**
>    d'accepter le paiement (**< 100 ms**) ;
> 2. **voie flux** : la capture des changements (**Debezium**) et les applications publient dans
>    **Kafka** ; des connecteurs alimentent Snowflake et MongoDB en **moins d'une minute** ;
> 3. **voie lots** : **Airflow** orchestre dbt, les tests, le rattrapage, le réentraînement et la
>    conformité (**toutes les 5 minutes** à **quotidien**).

## Plans

| Fichier | Question |
|---|---|
| [`00_plan_pipeline_cible.svg`](00_plan_pipeline_cible.svg) | comment la donnée circule : les trois chemins (synchrone, flux, lots) et leurs délais |
| [`11_dags_orchestration.svg`](11_dags_orchestration.svg) | dans quel ordre tournent les tâches des quatre DAG de la voie lots |
| [`12_flux_donnees_par_couche.svg`](12_flux_donnees_par_couche.svg) | où vit chaque table, qui la déplace, avec quel rôle |
| [`13_deploiement_supervision.svg`](13_deploiement_supervision.svg) | déploiement et supervision |
| [`14_resilience.svg`](14_resilience.svg) | ce qui se passe en cas de panne : 3 zones, bascule, région de secours |
| [`10_plan_pipeline_maquette.svg`](10_plan_pipeline_maquette.svg) | le pipeline reproduit dans la maquette |
| [`20_plan_pipeline_cible_et_maquette.svg`](20_plan_pipeline_cible_et_maquette.svg) | cible et maquette sur une même grille |


---

## 1. Principes

1. **Un chemin par délai** : ce qui doit être décidé tout de suite passe par le chemin synchrone,
   ce qui doit être partagé vite par la voie flux, ce qui doit être calculé et contrôlé par la
   voie lots.
2. **ELT** : charger le brut, transformer dans l'entrepôt. Une erreur se corrige en rejouant dbt.
3. **Une clé de réconciliation** : `transaction_id` relie l'OLTP, Kafka, l'OLAP et MongoDB.
4. **Une barrière qualité avant toute valorisation** : aucune donnée non validée n'atteint les
   tableaux de bord ni le rattrapage du modèle.
5. **Idempotence** : relire un message ou relancer une tâche ne crée ni doublon ni incohérence.
6. **Moindre privilège** : chaque composant a son rôle (livrable 6).
7. **L'échec est l'alerte** : une tâche en échec bloque l'aval et prévient l'équipe.

## 2. Les trois chemins

| Chemin | Délai | Ce qui y passe | Outils |
|---|---|---|---|
| **Synchrone** | < 100 ms (p99) | la décision de fraude, pendant le paiement | API de paiement → service de scoring (champion MLflow en mémoire) → OLTP |
| **Flux** | < 1 s vers Kafka ; < 1 min vers les destinations | chaque transaction validée, les événements applicatifs, les scores | Debezium → **Kafka** → connecteurs Snowflake et MongoDB |
| **Lots** | 5 min à 1 jour | transformations, tests, rattrapage, réentraînement, conformité, référentiels | **Airflow** → Airbyte, dbt, MLflow |

Pourquoi trois chemins et pas un seul : un traitement unique toutes les 5 minutes ne tient pas la
décision en 100 ms (livrable 0, conséquence C5) ; un tout-flux rendrait les transformations, les
tests et la conformité plus difficiles à rejouer et à auditer. Chaque chemin garde l'outil le
plus simple qui tient son délai.

## 3. Composants

| Étape | Outil cible | Pourquoi |
|---|---|---|
| Sources | OLTP PostgreSQL, applications web et mobiles, référentiels | l'application vit hors du pipeline |
| Haute disponibilité de l'OLTP | **Patroni** + etcd, **HAProxy** ; PostgreSQL 17 | bascule automatique sans perte, adresse unique pour les clients (§9.2) |
| Décision en ligne | **service de scoring** (API HTTP interne, plusieurs instances) | répondre en quelques millisecondes avec le modèle champion |
| Capture des changements (CDC) | **Debezium** sur Kafka Connect | lit le journal des transactions (WAL) : chaque insertion ou modification, dans l'ordre, sans requêter les tables |
| Bus d'événements | **Apache Kafka** (6 brokers, 3 zones) | découple producteurs et consommateurs ; conserve 7 jours, ce qui permet de rejouer |
| Contrat des messages | **registre de schémas** (Avro) | un producteur ne peut pas publier un message incompatible avec ses consommateurs |
| Chargement continu | **Snowpipe Streaming** (connecteur Kafka de Snowflake), connecteur Kafka de MongoDB | écrire en continu, sans code à maintenir |
| Ingestion par lots | **Airbyte** | référentiels et sources SaaS à évolution lente |
| Stockage analytique | **Snowflake** (RAW, STAGING, ANALYTICS, ML, AUDIT) | calcul séparé du stockage (livrable 3) |
| Stockage documents | **MongoDB** | événements semi-structurés, magasin de features (livrable 4) |
| Transformation (T) | **dbt** | SQL versionné, testé, dépendances calculées |
| Qualité | **dbt test** + **source freshness** | tests déclarés à côté des modèles |
| Valorisation | **MLflow** + **Evidently** | cycle de vie du modèle (livrable 7) |
| Exposition | outil de BI sur ANALYTICS | livrable 8 |
| Orchestration | **Apache Airflow** | ordre, planification, reprises, historique de la voie lots |
| Déploiement | **Docker** + **GitHub Actions** | même image du développement à la production |

> Kafka et Debezium ne font pas partie des outils pratiqués en formation : ils sont retenus parce
> que l'énoncé demande une synchronisation en temps réel et la capture des changements (CDC), et
> ils sont démontrés dans la maquette (§13).

## 4. Les flux

| # | De | Vers | Par | Chemin |
|---|---|---|---|---|
| 1 | API de paiement (features du paiement) | service de scoring | appel HTTP, délai maximal 300 ms | synchrone |
| 2 | API de paiement | OLTP : transaction **et son score** | transaction ACID | synchrone |
| 3 | OLTP : transactions, clients, commerçants, moyens de paiement | Kafka `cdc.oltp.*` | Debezium (journal WAL) | flux |
| 4 | API de paiement, applications | Kafka `events.risk_features`, `events.sessions`, `events.feedback`, `logs.app`, `fraud.scores` | producteurs Kafka | flux |
| 5 | Kafka `cdc.oltp.*`, `fraud.scores` | Snowflake `RAW`, `ML.FRAUD_SCORES` | Snowpipe Streaming | flux |
| 6 | Kafka `events.*`, `logs.app` | MongoDB (4 collections) | connecteur MongoDB, upsert | flux |
| 7 | référentiels : pays, taux de change | Snowflake `RAW` | Airbyte, rechargement complet, quotidien | lots |
| 8 | `RAW` + `ML.FRAUD_SCORES` | `STAGING` → `ANALYTICS` | dbt run, toutes les 5 min | lots |
| 9 | `ANALYTICS` | contrôle | source freshness + dbt test | lots |
| 10 | transactions validées **sans score** + `ml_features` | `ML.FRAUD_SCORES` + OLTP `fraud_score` | scoring de rattrapage | lots |
| 11 | `ANALYTICS` | BI | requêtes SQL | lots |

Détail table par table : [`12_flux_donnees_par_couche.svg`](12_flux_donnees_par_couche.svg).

## 5. Chemin synchrone : la décision de fraude

1. L'API de paiement calcule les features du paiement : montant, distance entre la localisation
   du paiement et celle du client, nombre de paiements du client dans l'heure. L'historique du
   client vient de son profil dans MongoDB (`customer_profiles`), mis à jour par la voie flux.
2. Elle appelle le **service de scoring**, qui garde le modèle **champion** en mémoire et le
   recharge quand l'alias change dans MLflow (promotion sans redéploiement).
3. Selon le score :
   - **< 0,9** → paiement accepté ;
   - **≥ 0,9** → **authentification renforcée** du client (3-D Secure) et file de **revue humaine**.
     Le modèle signale, il ne refuse pas seul (livrable 7 §6).
4. La transaction est enregistrée dans l'OLTP **avec son score**, dans la même transaction ACID.
5. Après validation, l'API publie les features et le score dans Kafka.

**Si le service de scoring ne répond pas en 300 ms** : l'API applique une **règle de repli**
(montant élevé ou forte vélocité → authentification renforcée), enregistre la transaction **sans
score**, et la voie lots la score au passage suivant (flux 10). Le paiement n'est jamais bloqué
par une panne du modèle.

## 6. Voie flux : CDC et Kafka

### 6.1 Capture des changements (CDC)

**Debezium** lit le **journal des transactions** de PostgreSQL (WAL), le même journal qui sert à
la réplication. Chaque transaction validée devient un message dans `cdc.oltp.transactions`, dans
l'ordre de validation.

| Point | Choix | Pourquoi |
|---|---|---|
| lecture | réplication logique, `pgoutput`, slot `cdc_slot` | aucune requête sur les tables : pas de charge supplémentaire sur l'OLTP |
| périmètre | publication `cdc_publication` sur 4 tables | seules les tables utiles quittent la base |
| minimisation RGPD | la publication **liste les colonnes** de `customers` : l'email n'est **jamais** publié | la donnée personnelle ne sort pas, même dans le journal |
| premier chargement | instantané initial, puis flux continu | la destination démarre complète |
| reprise | Debezium mémorise sa position (LSN) dans Kafka ; le slot conserve le journal tant qu'il n'est pas lu | une panne de Debezium ne perd aucun changement |
| modifications | les mises à jour (`UPDATE`) sont capturées | corrige la limite de l'ancienne extraction par `created_at` |

### 6.2 Kafka

| Topic | Producteur | Clé (fixe l'ordre) | Consommateurs |
|---|---|---|---|
| `cdc.oltp.transactions`, `.customers`, `.merchants`, `.payment_methods` | Debezium | clé primaire | Snowflake RAW |
| `events.risk_features` | API de paiement | `transaction_id` | MongoDB `ml_features` |
| `fraud.scores` | API de paiement | `transaction_id` | Snowflake `ML.FRAUD_SCORES` |
| `events.sessions` | applications | `customer_id` | MongoDB `user_sessions`, profils |
| `events.feedback` | applications | `customer_id` | MongoDB `customer_feedback` |
| `logs.app` | services | `transaction_id` | MongoDB `logs`, alertes de sécurité |

**Garanties de livraison :**
- **côté producteur** : `acks=all` et idempotence : un message confirmé est écrit sur les 3
  répliques, sans doublon en cas de nouvel envoi ;
- **côté consommateur** : la position (offset) n'est validée **qu'après** l'écriture dans la
  destination ; après une panne, le consommateur relit depuis le dernier message non écrit ;
- **destinations idempotentes** : upsert sur la clé, donc relire un message ne crée pas de
  doublon. Au total : chaque message produit son effet **une seule fois**.
- **ordre** : la clé envoie tous les messages d'une même transaction ou d'un même client dans la
  même partition, donc dans l'ordre.

**Rejeu :** 7 jours de rétention. Après un incident (destination corrompue, bug de chargement),
on remet la position du consommateur en arrière et on rejoue.

## 7. Voie lots : Airflow

### 7.1 Les quatre DAG

Détail : [`11_dags_orchestration.svg`](11_dags_orchestration.svg).

| DAG | Planification | Tâches | Garde-fous |
|---|---|---|---|
| `stripe_pipeline` | 5 min | `el_sync` (référentiels, Airbyte) → `dbt_transform` → `dbt_test_quality` → `fraud_scoring_catch_up` | `max_active_runs = 1` ; 2 tentatives à 2 min ; pas de champion → rattrapage *skipped* |
| `stripe_model_retraining` | quotidien ou à la demande | `check_drift` → `train_candidate` → `promote_if_better`, ou `no_retraining_needed` | entraînement reporté sous 200 transactions étiquetées ou 10 fraudes ; promotion conditionnelle (livrable 7) |
| `stripe_compliance_daily` | quotidien | `run_compliance_checks` → `assert_blocking_controls` | un contrôle bloquant en échec fait échouer le DAG (livrable 6) |
| `stripe_capacity` | toutes les heures | `check_capacity` : partitions à venir, débit de pointe comparé à la capacité, retard Kafka | au-delà de 70 % de la capacité, la tâche échoue : signal pour agrandir (§8.3) |

### 7.2 Transformation et qualité

`dbt run` construit, dans l'ordre des dépendances : `stg_transactions` (conversion USD, score
rattaché) → dimensions → `fact_transactions` → `agg_revenue_daily` (livrable 3). À fort volume,
les modèles sont **incrémentaux** : seules les nouvelles transactions sont traitées.

Puis `dbt source freshness` et `dbt test` : fraîcheur de RAW (avertissement 15 min, échec 60 min),
unicité et non-nullité des clés, relations faits → dimensions, valeurs admises, règles métier
(`amount_usd > 0`, `fraud_score` entre 0 et 1), réconciliation RAW ↔ étoile. **Un échec arrête le
DAG avant le rattrapage.**

### 7.3 Le scoring de rattrapage

Il ne traite que les transactions validées **restées sans score** (service de scoring indisponible
au moment du paiement), avec le même modèle et les mêmes features lues dans MongoDB. Il écrit
dans `ML.FRAUD_SCORES` et réécrit `fraud_score` dans l'OLTP (seule colonne autorisée). En
fonctionnement normal, il ne trouve rien à faire : c'est un filet de sécurité, mesuré (requête Q5).

## 8. Débit, latence et montée en charge

### 8.1 Délais obtenus

| Donnée | Délai | Chemin |
|---|---|---|
| décision de fraude | < 100 ms (p99) | synchrone |
| transaction dans Kafka | < 1 s après validation | flux |
| features et profil dans MongoDB | < 5 s | flux |
| transaction dans Snowflake RAW | < 1 min | flux |
| transaction dans l'étoile et les tableaux de bord | < 10 min | lots |
| score de rattrapage | ≤ 10 min | lots |
| rapport de conformité | quotidien | lots |

### 8.2 Dimensionnement de Kafka (livrable 0 §4.3)

≈ **30 Mo/s au pic** sur l'ensemble des topics ; 12 à 24 partitions par topic à fort débit ;
rétention 7 jours, ≈ **15 To** avec la réplication ×3 ; **6 brokers** répartis sur 3 zones.

### 8.3 Montée en charge

Toute la montée en charge est **horizontale** : on ajoute des machines, on ne remplace pas une
machine par une plus grosse.

| Composant | Montée en charge | Déclencheur | Index, partitionnement | Cache |
|---|---|---|---|---|
| Service de scoring | instances ajoutées automatiquement (sans état) | latence p99 > 80 ms ou CPU > 70 % | — | modèle en mémoire |
| OLTP | réplicas de lecture ; **répartition des écritures par commerçant** (Citus, livrable 2 §7.2) | 70 % de la capacité de 5 000 tx/s (≈ 13 mois, livrable 0 §4.2) | index ciblés ; **partitionnement mensuel dès le départ** | — |
| Kafka | brokers et partitions ajoutés ; consommateurs ajoutés jusqu'au nombre de partitions | retard des consommateurs (lag) en hausse durable | partitions par clé | — |
| Snowflake | **un entrepôt virtuel par usage** (chargement, transformation, BI, ML) ; multi-cluster quand les requêtes s'accumulent | file d'attente des requêtes | micro-partitions regroupées par date (clustering) | résultats (24 h), `agg_revenue_daily` |
| MongoDB | **sharding dès le départ** (≈ 16 shards, clé hachée) ; replica set par shard (livrable 4 §7) | > 70 % de stockage ou d'écritures par shard | index par motif ; purge automatique (TTL) | cache mémoire du moteur |
| dbt | modèles **incrémentaux** : seules les nouvelles transactions sont transformées | durée d'un passage > 2 min | — | — |
| Airflow | exécuteur distribué (Celery, Kubernetes) | tâches en attente | — | — |

**La décision d'agrandir repose sur une mesure, pas sur une impression.** Le DAG
`stripe_capacity` mesure chaque heure le débit de pointe de l'OLTP (meilleure minute de l'heure),
le compare à la capacité de conception et classe le résultat : **OK** sous 50 %, **VIGILANCE**
de 50 à 70 %, **AGRANDIR** au-delà, ce qui fait échouer la tâche et déclenche l'alerte. Il mesure
aussi le retard des consommateurs Kafka, crée les partitions mensuelles des trois mois à venir
et vérifie que la partition par défaut reste vide. Tout est historisé dans
`AUDIT.CAPACITY_REPORTS`.

### 8.4 Scénario ×10

25 000 tx/s au pic (livrable 0 §3) : l'architecture change de **taille**, pas de **forme**.

| Composant | Aujourd'hui | ×10 |
|---|---|---|
| Service de scoring | quelques instances | ×10 instances, sans changement de code |
| OLTP | 1 primaire + réplica synchrone | 8 à 16 serveurs Citus, chacun avec son réplica |
| Kafka | 6 brokers, 30 Mo/s | 12 à 18 brokers, 300 Mo/s ; partitions ×4 |
| MongoDB | ≈ 16 shards | ≈ 50 shards avec des conservations à chaud raccourcies |
| Snowflake | +12 To/an | +120 To/an ; entrepôts plus grands ou multi-cluster |
| dbt, Airflow | modèles incrémentaux | idem ; exécuteur Kubernetes |

### 8.5 Flexibilité : absorber un changement sans tout refaire

| Changement | Ce qui se passe | Ce qu'il faut faire |
|---|---|---|
| **nouvelle colonne** dans l'OLTP | Debezium la publie automatiquement ; le registre de schémas accepte un champ ajouté (compatibilité ascendante) ; les consommateurs ignorent ce qu'ils ne connaissent pas | l'ajouter à RAW et à dbt le jour où un usage le demande |
| nouvelle colonne **sur la table clients** | **pas publiée** : la publication liste ses colonnes une à une | décision explicite, après analyse RGPD |
| **nouveau type d'événement** | un nouveau topic | un connecteur vers la destination ; rien ne change ailleurs |
| **nouveau type de donnée** (audio, vidéo, document) | stockage objet + métadonnées dans MongoDB (comme les pièces de litige) | une collection ou un champ |
| **nouvelle source** (outil SaaS, fichier partenaire) | connecteur Airbyte ou producteur Kafka | un modèle dbt de staging |
| **nouveau modèle de ML** | même registre MLflow, même service de scoring, même surveillance | entraîner et promouvoir |
| **suppression ou changement de sens** d'un champ | **refusé** par le registre de schémas | nouvelle version du schéma (`schema_version`), migration planifiée |

Ce qui rend ces changements sûrs : les tests dbt (un champ attendu qui disparaît fait échouer la
voie lots avant les tableaux de bord) et le rejeu de Kafka (on peut recharger 7 jours après une
correction).

### 8.6 Mesuré dans la maquette

La maquette tourne sur un seul poste : elle ne prouve pas les volumes de la cible, elle prouve
les **mécanismes** et la **méthode de mesure**.

| Test | Résultat |
|---|---|
| Service de scoring, 48 clients en parallèle, **1 exemplaire** | 378 req/s, médiane 117 ms |
| Même test, **3 exemplaires** (`--scale scoring-api=3`) | **697 req/s**, médiane **44 ms** : le débit presque doublé |
| Chaîne complète (6 API × 500 paiements), 1 exemplaire du scoring | 265 tx/s ; scoring p99 24 ms ; RAW à jour 1,3 s après |
| Chaîne complète (12 × 500), 1 exemplaire | **plafond à 288 tx/s** : le scoring sature (p99 61 ms) |
| Chaîne complète (12 × 500), 3 exemplaires | 392 tx/s (16 × 1 000 : **460 tx/s**) ; RAW à jour 1,4 s après |
| Surveillance de capacité (capacité mesurée : 290 tx/s) | 142 tx/s → 49 %, **OK** ; après une charge plus forte, 267 tx/s → 92 %, **AGRANDIR** (tâche en échec) |
| Consommateurs Kafka (`--scale stream-processor=3`) | les 3 partitions de chaque topic réparties sur 3 consommateurs, retard nul |
| Partitionnement | une requête sur octobre 2026 ne lit que la partition d'octobre |
| Nouvelle colonne en marche | publiée dans Kafka en quelques secondes ; 0 erreur des consommateurs ; la colonne ajoutée aux clients n'est pas publiée |

**Lecture :** le goulot de la maquette est le service de scoring, et il se lève en ajoutant des
exemplaires ; la voie flux n'est jamais le goulot (RAW à jour en moins de 2 secondes). C'est la
même méthode qu'en production : mesurer, trouver le goulot, ajouter des machines à cet endroit.

## 9. Et si ça casse ? Continuité et reprise

Exigences du [livrable 0](../00_cahier_des_charges/README.md) §5 : chaîne de paiement disponible
à **99,999 %**, **RPO 0**, **RTO < 30 s** ; Kafka et MongoDB RPO 0 ; Snowflake RPO ≤ 1 h.
Schéma : [`14_resilience.svg`](14_resilience.svg).

### 9.1 Principes

1. **Aucun point unique de défaillance** sur la chaîne de paiement : chaque composant existe en
   au moins deux exemplaires, répartis sur **3 zones de disponibilité** (3 bâtiments).
2. **Détecter et basculer automatiquement, puis prévenir un humain** : la machine réagit en
   secondes, l'humain analyse ensuite.
3. **Une panne retarde, elle ne fait rien perdre** : RPO 0 sur les paiements ; la voie flux
   (Kafka, 7 jours) et la voie lots (dbt) se rejouent.
4. **Une architecture résiliente se prouve** : bascules et restaurations testées régulièrement.

### 9.2 Le cluster OLTP

| Élément | Cible | Pourquoi |
|---|---|---|
| nœuds | 1 primaire + 2 réplicas, un par zone | une zone entière peut tomber |
| réplication | **synchrone par quorum** : `synchronous_standby_names = 'ANY 1 (b, c)'` | chaque paiement validé existe sur 2 nœuds (RPO 0), et la panne d'un réplica ne bloque pas les écritures |
| bascule | **Patroni** + etcd (3 nœuds) : un verrou unique désigne le primaire ; s'il disparaît, le réplica le plus à jour est promu | automatique, en secondes ; jamais deux primaires (le verrou est unique) |
| adresse des clients | **HAProxy** interroge Patroni et n'envoie qu'au primaire | les clients n'ont qu'une adresse ; rien à reconfigurer après une bascule |
| capture des changements | PostgreSQL **17** : slot de Debezium en mode *failover*, recopié sur les réplicas | Debezium reprend sur le nouveau primaire ; sa position est réalignée sur le slot recopié (voir ci-dessous) |
| ancien primaire | isolé (*fencing*), puis reconstruit en réplica (`pg_rewind` ou copie complète) | il ne peut pas accepter d'écritures périmées |

**Ce que les tests de la maquette ont appris (et corrigé) :** après une bascule, la position
mémorisée par Debezium vient de l'ancien primaire et peut être légèrement en avance sur le
journal du nouveau. Debezium a ainsi sauté **450 transactions** écrites dans les 8 secondes
suivant la bascule. Correctif : après chaque bascule, la position mémorisée est effacée et
Debezium repart du **slot recopié**, qui est toujours en retard ou à égalité. Au pire, quelques
changements sont relus (sans effet grâce aux écritures idempotentes), aucun n'est sauté. Retesté :
3 152 transactions dans l'OLTP, 3 152 dans RAW, aucun doublon.

### 9.3 Matrice des pannes

| Panne | Détection | Réaction automatique | RPO | RTO | Impact métier | Testé dans la maquette |
|---|---|---|---|---|---|---|
| une instance du **service de scoring** | sonde de santé du répartiteur | trafic envoyé aux autres instances | 0 | < 1 s | aucun | — |
| **tout** le service de scoring | délai de 300 ms dépassé | **disjoncteur** ; décision par **règle de repli** ; score complété par le rattrapage | 0 | 0 (le paiement continue) | décisions moins fines pendant la panne | ✅ médiane 0 ms pendant la panne |
| **primaire OLTP** | gardien : 3 échecs consécutifs | promotion du réplica synchrone ; routeur redirigé ; Debezium réaligné ; alerte | **0** | **< 30 s** | paiements refusés quelques secondes, relancés par le client | ✅ **9 s**, 0 paiement perdu sur 3 052 |
| un **réplica OLTP** | gardien | cible : le quorum continue avec l'autre réplica ; maquette : passage en asynchrone (mode dégradé, alerte) | 0 (cible) | 0 | aucun | ✅ écritures suspendues 7,6 s, 0 refusée |
| **ancien primaire** qui revient | gardien | isolé : lecture seule, connexions coupées ; redémarré, il se reconstruit en réplica | — | — | aucun | ✅ gel puis réveil : isolé en 1 s |
| une **zone** entière | sondes de chaque cluster | chaque cluster (OLTP, Kafka, MongoDB) continue sur les 2 autres zones | 0 | < 1 min | ralentissement possible | ❌ (une seule machine) |
| un **broker Kafka** | contrôleur Kafka | les partitions sont reprises par leurs répliques (`min.insync.replicas = 2`) | 0 | secondes | aucun | ❌ (un seul nœud) |
| **Debezium** (Kafka Connect) | état des tâches | relance automatique ; le slot conserve le journal tant qu'il n'est pas lu | 0 | < 1 min | voie flux en retard | ✅ relance automatique |
| un **consommateur** ou une destination | retard (lag) | reprise à la dernière position validée | 0 | < 1 min | données en retard | ✅ 440 / 440 sans doublon |
| primaire d'un **shard MongoDB** | élection du replica set | un secondaire est élu primaire | 0 (`w: majority`) | < 15 s | profils clients momentanément indisponibles : décision sans profil | ❌ (un seul nœud) |
| **Snowflake** (région) | statut du service | bascule sur le compte répliqué (*failover group*) | ≤ 1 h | < 4 h | tableaux de bord en retard ; données rejouées depuis Kafka | ❌ |
| **Airflow** | supervision (maquette : contrôle de santé du planificateur toutes les 20 s) | redémarrage ; les passages manqués sont rattrapés | sans objet | < 15 min | étoile en retard | ✅ entrepôt (qui porte ses métadonnées dans la maquette) arrêté 75 s : relancé seul, sain 30 s après son retour |
| **MLflow** (registre) | sonde | le service de scoring garde le modèle en mémoire | sans objet | sans objet | aucun | ✅ (comportement du code) |
| **erreur humaine** (suppression, mise à jour fausse) | contrôles qualité, alertes | restauration à un instant précis (PITR) ; Time Travel Snowflake ; rejeu Kafka | minutes | heures | correction des données | ❌ |
| **rançongiciel** ou corruption | surveillance de sécurité | restauration depuis des sauvegardes **immuables** hors compte | minutes (journal archivé) | < 24 h | arrêt partiel | ❌ |
| **région** entière (sinistre) | décision humaine | **plan de reprise** : région secondaire (§9.4) | < 1 min | < 1 h | arrêt d'au plus une heure | ❌ |

### 9.4 Plan de reprise d'activité (PRA)

Une **région secondaire** est maintenue en veille active :

| Composant | Copie dans la région secondaire | Retard (RPO) |
|---|---|---|
| OLTP | réplica asynchrone inter-régions | secondes |
| Kafka | MirrorMaker 2 recopie les topics | secondes |
| MongoDB | membres du replica set dans la région secondaire (priorité 0 : jamais élus seuls) | secondes |
| Snowflake | réplication de base et *failover group* | ≤ 1 h |
| sauvegardes | copies chiffrées et immuables | 24 h (complètes) ; minutes (journal) |

**Procédure** (déclenchée par une décision humaine, car une bascule de région est lourde) :
promouvoir l'OLTP secondaire → basculer les clients Kafka sur le cluster secondaire → démarrer les
services (scoring, connecteurs, Airflow) → basculer l'adresse publique (DNS) → basculer Snowflake.
**RTO < 1 h, RPO < 1 min.**

### 9.5 Sauvegardes

| Système | Sauvegarde | Conservation |
|---|---|---|
| OLTP | pgBackRest : complète chaque semaine, différentielle chaque jour, journal (WAL) archivé en continu : restauration à la seconde près (PITR) | 35 jours ; copies chiffrées, immuables (verrouillage objet S3), dans une autre région |
| MongoDB | instantanés + journal des opérations (oplog) : restauration à un instant précis | 35 jours |
| Snowflake | Time Travel (retour en arrière dans le temps) + Fail-safe | 90 jours + 7 jours |
| Kafka | pas de sauvegarde : 7 jours rejouables ; ce sont les sources qui sont sauvegardées | — |

### 9.6 Prouver : les tests réguliers

| Test | Fréquence | Ce qu'il prouve |
|---|---|---|
| bascule du primaire OLTP (comme `ha/test_bascule.sh`) | mensuel | RTO et RPO réels, réalignement de la capture des changements |
| restauration d'une sauvegarde sur un environnement isolé | mensuel | la sauvegarde est lisible ; durée réelle de restauration |
| exercice de PRA (bascule de région) | annuel | la procédure fonctionne et est connue de l'équipe |
| pannes provoquées en préproduction (*chaos engineering*) | trimestriel | les réactions automatiques de la matrice |

### 9.7 Mesuré dans la maquette

Le cluster OLTP de la maquette a **deux nœuds PostgreSQL 17** (`oltp-db`, `oltp-replica`), un
**gardien** qui joue le rôle de Patroni ([`ha/failover_watchdog.py`](../maquette/ha/failover_watchdog.py)),
un **routeur** qui joue le rôle d'HAProxy ([`ha/primary_router.py`](../maquette/ha/primary_router.py))
et une **sonde** qui enregistre des paiements un par un pendant la panne
([`ha/sonde_bascule.py`](../maquette/ha/sonde_bascule.py), [`ha/test_bascule.sh`](../maquette/ha/test_bascule.sh)).

| Test | Résultat |
|---|---|
| panne brutale du primaire (`docker compose kill`) | bascule en **8,9 s** (détection 6 s, promotion 3 s) ; interruption vue par l'API : **9,2 s** |
| RPO | **3 052 paiements confirmés, 0 absent** du nouveau primaire |
| capture des changements | Debezium relancé et réaligné automatiquement ; RAW = OLTP (3 152), **0 doublon** |
| panne du réplica | écritures suspendues **7,6 s**, puis passage en asynchrone (alerte) ; 0 paiement refusé |
| ancien primaire gelé puis réveillé | **isolé en lecture seule** en 1 s ; redémarré, reconstruit en réplica |
| retour à la normale | réplica réintégré, réplication **synchrone** rétablie automatiquement |

Ce que la maquette ne reproduit pas : 3 zones, quorum à deux réplicas (un seul réplica ici, d'où
le mode dégradé), Patroni et etcd, la région secondaire, les sauvegardes.

## 10. Cohérence et synchronisation

### 10.1 Une source de vérité par donnée

| Donnée | Source de vérité | Copies |
|---|---|---|
| transactions, clients, commerçants | OLTP | Kafka, RAW, étoile |
| événements, features | Kafka puis MongoDB | aucune |
| score de fraude | OLTP (`fraud_score`, écrit au paiement) | `fraud.scores`, `ML.FRAUD_SCORES`, `fact_transactions` |
| référentiels | fichiers et API de référence | RAW |

### 10.2 Conflits : évités par construction

**Un seul écrivain par donnée** : l'API de paiement écrit la transaction et son score ; le
rattrapage n'écrit que les scores manquants (`fraud_score IS NULL`), avec un droit limité à cette
colonne ; les connecteurs n'écrivent que RAW, ML et MongoDB ; dbt que STAGING et ANALYTICS. Un
score est écrit une seule fois par transaction (`ON CONFLICT DO NOTHING`) ; le rescorer est une
décision explicite, tracée par `model_version`.

### 10.3 Cohérence à terme, assumée et mesurée

L'OLTP est à jour immédiatement ; Kafka en moins d'une seconde ; MongoDB en quelques secondes ;
Snowflake RAW en moins d'une minute ; l'étoile en moins de 10 minutes. Ces écarts sont **mesurés**
(retard des consommateurs Kafka, fraîcheur dbt, requête Q5).

### 10.4 Double écriture

L'API écrit dans l'OLTP puis publie dans Kafka : si elle s'arrête entre les deux, les événements
applicatifs de cette transaction manquent. La transaction elle-même n'est pas perdue (Debezium la
lit dans le journal) et son score non plus (il est dans l'OLTP ; le rattrapage complète
`ML.FRAUD_SCORES`). Évolution possible : le motif *outbox*, où l'API écrit aussi ses événements
dans une table de l'OLTP que Debezium publie.

## 11. Gestion des erreurs et des exceptions

### 11.1 Deux familles d'erreurs, deux réponses

| Famille | Exemples | Réponse | Pourquoi |
|---|---|---|---|
| **passagère** : un composant est indisponible | base redémarrée, réseau coupé, élection d'un primaire MongoDB | **nouvel essai**, avec une attente qui double à chaque échec (1, 2, 4… 30 s) | la donnée n'est pas en cause : elle passera dès le retour du composant ; l'attente croissante évite d'accabler un service qui redémarre |
| **définitive** : la donnée elle-même est invalide | JSON illisible, montant « abc », champ obligatoire absent, valeur refusée par une contrainte | **quarantaine** : l'enregistrement est mis de côté avec sa raison, le flux continue | réessayer ne servirait à rien et bloquerait tous les messages suivants (« message empoisonné ») |

Aucune erreur n'est **silencieuse** : chaque rejet est conservé et chaque incident déclenche une
**alerte** (§11.4).

### 11.2 Où chaque erreur est traitée

| Étape | Contrôle | En cas d'erreur définitive | En cas d'indisponibilité |
|---|---|---|---|
| API de paiement → scoring | délai de 300 ms, réponse 200 attendue | règle de repli (le paiement continue) | disjoncteur : 3 échecs → 30 s sans appel ; alerte à l'ouverture et à la fermeture |
| service de scoring | features présentes et numériques ; résultat du modèle | réponse 400 (requête invalide) ou 500 (modèle défaillant) : l'API applique la règle de repli | modèle gardé en mémoire si le registre MLflow est injoignable |
| API → Kafka | confirmation de chaque message (`acks=all`) | — | alerte si des événements ne sont pas confirmés |
| Kafka → RAW, MongoDB, `ML` | contrat de chaque message (types, champs obligatoires, plage du score) **avant** l'écriture | **quarantaine** ; si la base refuse un lot, réécriture ligne par ligne pour n'isoler que la ou les lignes fautives | nouvel essai avec attente croissante ; l'offset n'avance pas ; **alerte CRITICAL après 5 échecs**, alerte INFO au retour |
| référentiels (CSV) | chaque ligne : format des codes, date, taux positif | ligne en **quarantaine** (une seule fois, même si le fichier est relu) ; **plus de 10 % de lignes invalides : fichier refusé, l'ancienne version reste en place** | nouvel essai |
| dbt (règles métier) | montant, taux de change, client et commerçant connus | transaction en **quarantaine métier** (`quarantine_transactions`) au lieu de disparaître dans une jointure ; elle en sort d'elle-même si la donnée manquante arrive | — |
| dbt (seuils) | quarantaine de plus de 10 min ; part du jour en quarantaine | avertissement ; **au-delà de 1 % : la voie lots s'arrête** avant le rattrapage, alerte | — |
| tâches Airflow | délai maximal par tâche (une tâche bloquée est arrêtée) | — | nouvel essai avec attente croissante (1, 2, 4 min…) ; **alerte CRITICAL** après la dernière tentative |
| cluster OLTP | gardien (§9.2) | — | bascule ; alertes de bascule, de mode dégradé, d'isolement, de relance de Debezium |

### 11.3 La quarantaine

| | Voie flux et référentiels | Règles métier |
|---|---|---|
| où | `RAW._QUARANTINE` (Snowflake) ; en cible, file d'erreurs de Kafka Connect (`errors.deadletterqueue.topic.name`) | `ANALYTICS.QUARANTINE_TRANSACTIONS` (dbt) |
| contenu | source, position (partition et offset, ou numéro de ligne), message brut, raison | transaction, raison |
| traitement | correction de la donnée ou du programme, puis **rejeu** (re-publication dans Kafka ou rechargement) ; `replayed_at` trace le rejeu | correction du référentiel ou de la donnée source ; la transaction rejoint l'étoile au passage suivant |
| suivi | alerte à chaque mise en quarantaine | test dbt : avertissement si une transaction y reste plus de 10 minutes, échec au-delà de 1 % |

### 11.4 Les alertes

Toutes les alertes suivent le même chemin : **journal du service**, table **`AUDIT.ALERTS`**
(rôle `alerter`, qui ne peut qu'y ajouter des lignes), et **outil d'astreinte** (Slack,
PagerDuty) en cible. Trois niveaux : **INFO** (retour à la normale), **WARNING** (quarantaine,
mode dégradé, circuit ouvert), **CRITICAL** (tâche en échec définitif, consommateur bloqué,
bascule). Une alerte qui ne peut pas être envoyée ne fait jamais échouer le traitement qui
l'émet.

### 11.5 Tests automatisés

32 tests (`maquette/tests/`, pytest) vérifient les cas d'erreur : messages invalides refusés avec
la bonne raison, séparation des bons et des mauvais messages d'un même lot, classement
passager/définitif des erreurs, refus par la base isolé à la ligne près (test d'intégration sur
l'entrepôt), validation des référentiels, disjoncteur, réponses 400, 500 et 503 du service de
scoring, seuils de capacité, alerte qui ne bloque jamais l'appelant.

### 11.6 Mesuré dans la maquette

| Test | Résultat |
|---|---|
| 3 messages invalides injectés dans Kafka (`queries/08_injection_erreurs.sh`) | 3 rejets en quarantaine avec leur raison exacte, flux non interrompu, 3 alertes WARNING |
| transaction dont le client n'existe pas | isolée par dbt (« client inconnu »), tests dbt au vert |
| fichier de taux de change avec 3 lignes fausses sur 4 | fichier refusé, **les 8 anciens taux restent en place**, 3 lignes en quarantaine (avec leur numéro), alerte CRITICAL après la dernière tentative |
| entrepôt arrêté 75 s | nouveaux essais à 1, 2, 4, 8, 16, 30 s ; alerte CRITICAL au 5e échec, INFO au retour ; **OLTP 1 000 = RAW 1 000** |
| 32 tests automatisés | tous au vert |

## 12. Déploiement et supervision

Voir [`13_deploiement_supervision.svg`](13_deploiement_supervision.svg).

**Déploiement** : code versionné dans Git ; **GitHub Actions** teste, construit l'image **Docker**
et la déploie. Kafka managé (Confluent Cloud ou Amazon MSK) avec Kafka Connect pour Debezium et
les connecteurs ; service de scoring sur un groupe d'instances à mise à l'échelle automatique ;
Airflow sur un serveur dédié ; Snowflake, MongoDB et PostgreSQL managés ; MLflow sur Hugging Face
Spaces (base PostgreSQL, artefacts S3). Un secret par rôle, dans un coffre.

**Supervision** :

| Quoi | Comment | Alerte |
|---|---|---|
| décision de fraude | latence p99 du service de scoring, part des décisions par repli | p99 > 80 ms ; repli > 1 % |
| capacité | DAG `stripe_capacity` : débit de pointe / capacité, retard Kafka, partitions à venir | > 70 % : agrandir ; partition par défaut non vide |
| cluster OLTP | Patroni : rôle de chaque nœud, retard de réplication, mode synchrone | bascule effectuée ; réplica absent (mode dégradé) ; retard > 1 s |
| données refusées | `RAW._QUARANTINE`, `QUARANTINE_TRANSACTIONS`, tests dbt | toute mise en quarantaine ; quarantaine de plus de 10 min ; plus de 1 % du jour |
| toutes les alertes | `AUDIT.ALERTS` + outil d'astreinte (§11.4) | CRITICAL : appel de l'astreinte ; WARNING : traitement le jour même |
| voie flux | état des connecteurs, **retard des consommateurs** (lag) | connecteur arrêté ; lag > 1 min |
| exécution des tâches | interface Airflow | échec après les tentatives |
| fraîcheur | `dbt source freshness`, état des flux | 15 min (avert.), 60 min (échec) |
| qualité | `dbt test`, réconciliation RAW ↔ étoile | échec = rattrapage bloqué |
| modèle | MLflow : métriques, dérive, versions | dérive → réentraînement |
| conformité | rapport quotidien historisé | contrôle bloquant en échec |
| accès | journaux des bases | revue dans le rapport de conformité |

**Ce qui change dans le code entre la maquette et la cible** : les consommateurs Python sont
remplacés par des connecteurs déclarés, et dbt pointe sur Snowflake. Les deux extraits ci-dessous
montrent ce qui changerait ; ils ne sont pas exécutés dans ce projet.

```json
{
  "name": "snowflake-raw",
  "config": {
    "connector.class": "com.snowflake.kafka.connector.SnowflakeSinkConnector",
    "snowflake.ingestion.method": "SNOWPIPE_STREAMING",
    "topics": "cdc.oltp.transactions,cdc.oltp.customers,cdc.oltp.merchants,cdc.oltp.payment_methods",
    "snowflake.topic2table.map": "cdc.oltp.transactions:TRANSACTIONS,cdc.oltp.customers:CUSTOMERS,cdc.oltp.merchants:MERCHANTS,cdc.oltp.payment_methods:PAYMENT_METHODS",
    "snowflake.database.name": "STRIPE_DWH",
    "snowflake.schema.name": "RAW",
    "snowflake.role.name": "LOADER",
    "snowflake.user.name": "SVC_LOADER",
    "snowflake.private.key": "${file:/secrets/snowflake.properties:private_key}"
  }
}
```

```yaml
# dbt_stripe/profiles.yml, cible Snowflake (la maquette utilise type: postgres)
dbt_stripe:
  target: prod
  outputs:
    prod:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      role: TRANSFORMER
      warehouse: WH_TRANSFORMING
      database: STRIPE_DWH
      schema: STAGING
      threads: 8
```

## 13. Dans la maquette

La [maquette](../maquette/README.md) reproduit **les trois chemins**, dans le même ordre, avec les
mêmes couches, les mêmes topics, les mêmes rôles et les mêmes tests. Substitutions :

| Cible | Maquette | Raison |
|---|---|---|
| Kafka (6 brokers) | **Redpanda**, un nœud : même protocole que Kafka, les clients Kafka et Debezium s'y connectent tels quels | un seul exécutable, prêt en quelques secondes, peu de mémoire |
| Debezium sur Kafka Connect | **Debezium** sur Kafka Connect — identique | — |
| Snowpipe Streaming, connecteur MongoDB | consommateurs Python (`pipeline/stream_processor.py`) : même garantie (offset validé après écriture, upsert) | connecteurs payants ou propres au cloud |
| service de scoring | `scoring-api` (Flask, gunicorn) : même rôle, champion rechargé à la promotion | — |
| Patroni + etcd (3 nœuds OLTP sur 3 zones) | **gardien** Python (`ha/failover_watchdog.py`), 2 nœuds PostgreSQL 17 | Patroni demande etcd et une image dédiée ; le gardien applique les mêmes règles (synchrone, bascule, isolement) |
| HAProxy | **routeur** Python (`ha/primary_router.py`) | même rôle : une adresse, toujours vers le primaire |
| API de paiement, applications | simulateur : appelle le scoring, écrit dans l'OLTP, publie dans Kafka | données synthétiques |
| Airbyte | tâche Python `el_sync`, rechargement complet des référentiels | Airbyte demande une dizaine de conteneurs |
| Snowflake | PostgreSQL, mêmes couches et tables | service cloud payant |
| Evidently | PSI par variable, suivi dans MLflow | même décision, sans dépendance |
| outil de BI | requêtes SQL (rôle `data_analyst`) | mêmes questions métier |
| registre de schémas (Avro) | messages JSON | simplification |
| serveurs managés, CI/CD | Docker Compose | démonstration locale |

Reproduits en plus : la **gestion des erreurs** (quarantaine, alertes, 32 tests automatisés, §11.6), la **réplication synchrone et la bascule automatique** de l'OLTP (§9.7), le **partitionnement mensuel** de l'OLTP, la **surveillance de capacité**, la
**mise à l'échelle** du scoring et des consommateurs (`docker compose up --scale`), le test de
charge (`charge/`) et l'ajout de colonne à chaud (§8.6).

Non reproduits : TLS et chiffrement au repos, plusieurs brokers et zones, sharding de l'OLTP et de
MongoDB (plusieurs serveurs), déploiement continu.
