# Livrable 0 : cahier des charges chiffré

**Rôle de ce document :** fixer, *avant* toute conception, les exigences que l'architecture doit
tenir. Il répond, dans cet ordre, aux quatre questions qu'on pose à toute architecture de données :

| # | Question | Ce qu'on fixe | Section |
|---|---|---|---|
| Q1 | **Quoi ?** | les types de données et le système qui convient à chacun (*variété*) | §2 |
| Q2 | **Combien ?** | le nombre de transactions et le volume stocké, aujourd'hui et demain (*volume*) | §3 |
| Q3 | **À quelle vitesse ?** | le débit à absorber et les délais à respecter (*vélocité*) | §4 |
| Q4 | **Et si ça casse ?** | la disponibilité exigée et les pertes acceptables (RPO, RTO) | §5 |

Chaque choix des livrables 1 à 8 se justifie par un chiffre de ce document (§6).

> **En bref**
> - **Quoi ?** Des données structurées (PostgreSQL, Snowflake), semi-structurées (JSON, XML, Avro :
>   MongoDB, Kafka) et non structurées (texte libre : MongoDB ; fichiers binaires : stockage objet).
> - **Combien ?** **27,5 milliards** de transactions par an, **+34 % par an**. Volume chaud :
>   **≈ 7,5 To** dans l'OLTP, **≈ 40 To** dans MongoDB, **+12 To/an** dans Snowflake, **27 To/an**
>   de pièces binaires.
> - **À quelle vitesse ?** **870 tx/s** en moyenne, **2 530 tx/s** au pic, capacité de conception
>   **5 000 tx/s**, atteinte en 2 à 3 ans. Décision de fraude en **moins de 100 ms**, tableaux de
>   bord frais en **moins de 10 minutes**.
> - **Et si ça casse ?** Chaîne de paiement disponible à **99,999 %** (≈ 5 minutes d'arrêt par an),
>   **aucune transaction validée perdue** (RPO = 0), reprise en **moins de 30 secondes** (RTO).

---

## 1. Méthode

Chaque chiffre suit la même chaîne : **source → hypothèse → calcul → conséquence sur la conception**.

- Les **sources** sont les chiffres publics de Stripe et les textes réglementaires (liste en fin
  de document).
- Les **hypothèses** sont signalées `H1`, `H2`… : elles sont raisonnables mais propres à cette étude.
  Si une hypothèse change, le calcul se refait et la conséquence se relit.
- Les volumes sont donnés **hors réplication** sauf mention contraire.

**Sources de départ :**

| # | Donnée | Valeur | Origine |
|---|---|---|---|
| S1 | volume de paiement 2025 | 1 900 Md$ (+34 % sur 2024) | lettre annuelle Stripe 2025 |
| S2 | Black Friday → Cyber Monday 2025 | 578 M de transactions en 4 jours, 40 Md$ | Stripe Newsroom |
| S3 | pic observé | 152 000 transactions par minute | Stripe Newsroom |
| S4 | disponibilité de l'API | > 99,999 % sur la période | guide Stripe « peak season 2026 » |

---

## 2. Q1 — Quoi ? Les types de données et leur système

| Donnée | Type | Format | Système | Traitement |
|---|---|---|---|---|
| transactions, clients, commerçants | structurée | lignes relationnelles | PostgreSQL | contraintes, ACID |
| référentiels (pays, taux de change) | structurée | CSV, API | Snowflake RAW | rechargement complet |
| historique analytique | structurée | colonnes | Snowflake | étoile, pré-agrégats |
| événements en transit | semi-structurée | Avro, schéma versionné | Kafka | registre de schémas : compatibilité vérifiée |
| sessions et clics | semi-structurée | JSON imbriqué | MongoDB | validation `$jsonSchema`, index |
| journaux | semi-structurée | JSON | MongoDB | TTL 30 jours |
| features ML | semi-structurée | JSON | MongoDB | index unique `transaction_id` |
| messages bancaires de litige | semi-structurée | **XML** | MongoDB | champs utiles extraits + message d'origine conservé |
| avis clients | **non structurée** | texte libre | MongoDB | index texte ; analyse de sentiment en lot |
| pièces justificatives | **non structurée** | **binaire** (PDF, images) | stockage objet S3 | chiffrées ; référence dans MongoDB |

**Règle :** chaque type va dans le système optimisé pour lui ; `transaction_id` relie les
systèmes entre eux.

---

## 3. Q2 — Combien ? Transactions et volume stocké

### 3.1 Nombre de transactions

| # | Hypothèse |
|---|---|
| H1 | panier moyen de **69 $** (= 40 Md$ ÷ 578 M, calculé à partir de S2), supposé stable sur l'année |

| Indicateur | Calcul | Résultat |
|---|---|---|
| transactions par an | 1 900 Md$ ÷ 69 $ | **≈ 27,5 Md** |
| transactions par jour (moyenne) | 27,5 Md ÷ 365 | **≈ 75 M** |
| jour de pic | 578 M ÷ 4 | ≈ 145 M (×2 la moyenne) |
| croissance | S1 | **+34 % par an**, ×2,4 en 3 ans |

> L'énoncé dit « des milliards de transactions par an » et « des millions par jour » : les deux
> ordres de grandeur sont respectés.

### 3.2 Durées de conservation imposées

Elles fixent combien de temps les données restent stockées, donc le volume total.

| Donnée | Durée | Texte |
|---|---|---|
| pièces comptables (transactions) | 10 ans | Code de commerce, art. L123-22 |
| données d'identification et opérations (lutte anti-blanchiment) | 5 ans | Code monétaire et financier, art. L561-12 |
| journaux d'audit | 12 mois, dont 3 immédiatement consultables | PCI DSS v4.0, exigence 10.5.1 |
| traceurs de navigation | 13 mois (traceur), 25 mois (données collectées) | recommandation CNIL |
| journaux techniques | 30 jours | minimisation (RGPD, art. 5) : pas de besoin au-delà |

### 3.3 OLTP (PostgreSQL)

| # | Hypothèse |
|---|---|
| H2 | une transaction du modèle (livrable 2) occupe **≈ 0,25 Ko** avec ses index (≈ 130 octets de ligne + index de clé primaire, commerçant/date, client, partiel) |
| H3 | conservation **à chaud 13 mois** (fenêtre des remboursements et litiges) ; au-delà, la partition est détachée et archivée, l'historique complet vit dans Snowflake |

| Indicateur | Calcul | Résultat |
|---|---|---|
| croissance | 75 M × 0,25 Ko | **≈ 19 Go/jour**, ≈ 7 To/an |
| volume chaud | 13 mois | **≈ 7,5 To** (×3 avec deux réplicas) |

### 3.4 MongoDB (NoSQL)

| Collection | Hypothèse | Par jour | Conservation | Volume chaud |
|---|---|---|---|---|
| `user_sessions` | H4 : 0,5 session par transaction, 15 événements imbriqués, 3 Ko | 113 Go | 90 jours (puis agrégée dans `customer_profiles` et archivée dans Snowflake) | 10 To |
| `logs` | H5 : 10 lignes de journal par transaction, 0,5 Ko | 377 Go | 30 jours (TTL) | 11 To |
| `ml_features` | 1 document par transaction, 1 Ko | 75 Go | 180 jours (fenêtres d'entraînement) | 14 To |
| `customer_feedback` | H6 : 0,2 % des transactions, 2 Ko | 0,3 Go | 3 ans | 0,3 To |
| `dispute_evidence` | H7 : 0,1 % des transactions en litige, 20 Ko de métadonnées et de XML | 1,5 Go | 5 ans | 2,7 To |
| `customer_profiles` | H8 : 300 M de clients actifs, 5 Ko | — | durée de la relation | 1,5 To |
| **Total** | | **≈ 570 Go/jour** | | **≈ 40 To** (≈ 52 To avec index) |

Les **pièces justificatives binaires** des litiges (PDF, photos, ≈ 1 Mo chacune) représentent
**75 Go/jour, soit 27 To/an** : elles vont dans un **stockage objet chiffré** (S3) et MongoDB ne
garde que leur référence (livrable 4).

### 3.5 OLAP (Snowflake)

| Couche | Calcul | Par an (compressé ×5, stockage en colonnes) |
|---|---|---|
| RAW transactions (enveloppe complète) | 27,5 Md × 0,5 Ko ÷ 5 | 2,8 To |
| table de faits | 27,5 Md × 100 octets ÷ 5 | 0,6 To |
| sessions archivées | 113 Go × 365 ÷ 5 | 8,2 To |
| **Total** | | **≈ 12 To/an**, conservés 10 ans (§3.2) |

---

## 4. Q3 — À quelle vitesse ? Débit et délais

La vitesse a deux faces : le **débit** (combien d'éléments par seconde) et la **latence** (en
combien de temps un élément doit être traité).

### 4.1 Débit des transactions

| Indicateur | Calcul | Résultat |
|---|---|---|
| débit moyen | 75 M ÷ 86 400 s | **≈ 870 tx/s** |
| débit de pic | 152 000 ÷ 60 (S3) | **≈ 2 530 tx/s** (×3 la moyenne) |
| **capacité de conception** | pic × 2 (marge de sécurité) | **5 000 tx/s soutenus** |
| seuil d'alerte | 70 % de la capacité | 3 500 tx/s |

**Pourquoi une marge ×2 et un seuil à 70 % :** un système proche de 100 % de sa capacité voit ses
files d'attente, donc sa latence, augmenter fortement. On vise un fonctionnement normal vers 50 %
et on déclenche l'agrandissement à 70 %.

### 4.2 Évolution du débit

| Horizon | Facteur (+34 %/an) | Pic attendu | Lecture |
|---|---|---|---|
| aujourd'hui | ×1 | 2 530 tx/s | 51 % de la capacité |
| an 1 | ×1,34 | 3 400 tx/s | 68 % : le **seuil d'alerte de 70 %** est atteint vers 13 mois, on lance la montée en charge |
| an 2 | ×1,80 | 4 550 tx/s | 91 % : la montée en charge doit être en service |
| an 3 | ×2,41 | 6 100 tx/s | capacité initiale dépassée |
| scénario de stress | ×10 | 25 000 tx/s | test de la stratégie de montée en charge |

**Conséquence :** un primaire PostgreSQL unique suffit au démarrage, mais pas au-delà de 2 ans. La
répartition horizontale (sharding) de l'OLTP est **conçue maintenant** et déclenchée par un seuil
mesuré, pas improvisée le jour où la base sature (livrable 5 §8.3).

### 4.3 Débit des autres flux

| Flux | Moyenne | Pic (×3) |
|---|---|---|
| écritures OLTP | 870 /s | 2 530 /s (capacité 5 000) |
| événements de session (MongoDB) | ≈ 6 500 /s | ≈ 20 000 /s |
| lignes de journal (MongoDB) | ≈ 8 700 /s | ≈ 26 000 /s |

**Kafka (bus d'événements) :**

| Topic | Débit au pic | Taille | Flux au pic | Partitions |
|---|---|---|---|---|
| `cdc.oltp.transactions` (Debezium) | 5 000 msg/s | 1 Ko | 5 Mo/s | 12 |
| `cdc.oltp.customers`, `.merchants`, `.payment_methods` | < 100 msg/s | 0,5 Ko | < 0,1 Mo/s | 3 |
| `events.risk_features` | 5 000 msg/s | 1 Ko | 5 Mo/s | 12 |
| `events.sessions` (clics) | 20 000 msg/s | 0,3 Ko | 6 Mo/s | 24 |
| `events.feedback` | 10 msg/s | 2 Ko | < 0,1 Mo/s | 3 |
| `logs.app` | 26 000 msg/s | 0,5 Ko | 13 Mo/s | 24 |
| `fraud.scores` | 5 000 msg/s | 0,2 Ko | 1 Mo/s | 12 |
| **Total** | | | **≈ 30 Mo/s** | |

Rétention de **7 jours** (temps de rejouer un incident) au débit moyen de ≈ 8 Mo/s, soit
≈ 5 To, **≈ 15 To avec un facteur de réplication de 3**, répartis sur 6 brokers dans 3 zones de
disponibilité. Le nombre de partitions vise ≈ 400 à 1 000 messages/s par consommateur, ce qui laisse
de la place pour ajouter des consommateurs.

### 4.4 Délais exigés (latence)

| Flux | Latence exigée | Pourquoi | Mécanisme (livrable 5) |
|---|---|---|---|
| décision de fraude | **p99 < 100 ms** | décider *avant* d'autoriser le paiement | service de scoring appelé par l'API de paiement |
| OLTP → bus d'événements | **< 1 s** | tous les systèmes voient la transaction sans attendre | CDC (Debezium) → Kafka |
| bus → MongoDB (features, profils) | **< 5 s** | la transaction suivante du même client en tient compte | consommateur Kafka |
| bus → Snowflake RAW | **< 1 min** | données brutes disponibles pour l'analyse | Snowpipe Streaming |
| RAW → tableaux de bord | **< 10 min** | « quasi temps réel » demandé par l'énoncé | dbt toutes les 5 minutes, puis tests |
| alertes de sécurité | **< 1 min** | limiter l'impact d'une intrusion | règles sur le flux de journaux |
| rapports de conformité | **quotidien (J+1)** | rythme des contrôles et des autorités | DAG Airflow quotidien |
| référentiels (pays, taux) | quotidien | données à évolution lente | Airbyte, par lots |

**Conséquence :** un traitement unique par lots toutes les 5 minutes (version précédente) ne tient
pas l'exigence de 100 ms. L'architecture a donc **deux voies** : une **voie flux** pour ce qui doit
être immédiat et une **voie lots** pour l'analytique et les contrôles (livrable 5).

---

## 5. Q4 — Et si ça casse ? Disponibilité et pertes acceptables

**RPO** (*Recovery Point Objective*) : quantité de données qu'on accepte de perdre lors d'une panne.
**RTO** (*Recovery Time Objective*) : durée d'interruption acceptable.

| Système | Disponibilité | RPO | RTO | Pourquoi ce niveau |
|---|---|---|---|---|
| chaîne de paiement (API, OLTP, scoring) | **99,999 %** (≈ 5 min/an, S4) | **0** | **< 30 s** | une transaction perdue est de l'argent perdu |
| Kafka | 99,99 % | **0** | < 1 min | il transporte les transactions validées |
| MongoDB | 99,99 % | 0 (écriture majoritaire) | < 15 s | features lues pendant le scoring |
| Snowflake | 99,9 % | ≤ 1 h | < 4 h | analytique ; les données se rejouent depuis Kafka ou l'OLTP |
| Airflow | 99,9 % | sans objet | < 15 min | le retard se rattrape au redémarrage |

Le détail des pannes possibles, la bascule, le plan de reprise et les sauvegardes sont dans le
livrable 5 §9. **Mesuré dans la maquette** (livrable 5 §9.7) : panne brutale du primaire, aucun
paiement confirmé perdu (RPO 0 vérifié sur 3 052 paiements), écritures interrompues 9 s.

---

## 6. Conséquences sur la conception

| # | Question | Le chiffre | impose | Livrable |
|---|---|---|---|---|
| C1 | Quoi ? | 27 To/an de pièces binaires | **stockage objet** plutôt que GridFS | 4 |
| C2 | Combien ? | 7 To/an dans l'OLTP | **partitionnement mensuel dès le départ** et archivage des partitions de plus de 13 mois | 2 |
| C3 | Combien ? | 40 To chauds dans MongoDB | **sharding dès le départ** (≈ 16 shards de 3 nœuds) | 4 |
| C4 | Combien ? | 377 Go/jour de journaux | TTL 30 jours ; journaux de sécurité séparés, 12 mois en stockage immuable | 4, 6 |
| C5 | Vitesse ? | décision de fraude < 100 ms | une **voie flux** (CDC + Kafka) et un scoring en ligne, en plus des lots | 5, 7 |
| C6 | Vitesse ? | +34 %/an, seuil de 70 % vers 13 mois | **sharding de l'OLTP par commerçant**, déclenché à 70 % de capacité ; identifiants générés sans séquence centrale | 2, 5 |
| C7 | Vitesse ? | 30 Mo/s dans Kafka, 7 jours | 6 brokers, réplication ×3, partitions dimensionnées | 5 |
| C8 | Si ça casse ? | 99,999 %, RPO 0 | **réplication synchrone et bascule automatique** sur chaque composant de la chaîne de paiement, 3 zones de disponibilité | 2, 4, 5 |

## 7. Ce que la maquette reproduit

La maquette fonctionne à quelques transactions par seconde sur un poste local : elle ne prouve pas
les volumes ci-dessus. Elle prouve les **mécanismes** qui permettent de les atteindre : mêmes types
de données et mêmes systèmes (Q1), même chemin de la donnée et même voie flux grâce à un
équivalent local de Kafka (Q3), même bascule de la base principale vers son réplica et mêmes
traitements d'erreur (Q4). Elle **mesure aussi son propre débit** (lignes par seconde, durée de
chaque étape) : c'est la méthode que l'on appliquerait en production pour savoir où l'on se situe
par rapport au seuil de 70 % (Q3).

## Sources

- Stripe, *Stripe publishes 2025 annual letter* : https://stripe.com/newsroom/news/stripe-2025-update
- Stripe, *Businesses on Stripe processed more than $40 billion from Black Friday through Cyber
  Monday* : https://stripe.com/newsroom/news/bfcm2025
- Stripe, *Preparing for peak shopping season 2026* : https://stripe.com/guides/preparing-for-peak-shopping-season-2026
- Code de commerce, art. L123-22 ; Code monétaire et financier, art. L561-12 (Légifrance)
- PCI Security Standards Council, *PCI DSS v4.0*, exigence 10.5.1
- CNIL, lignes directrices et recommandation « cookies et autres traceurs » (2020)
