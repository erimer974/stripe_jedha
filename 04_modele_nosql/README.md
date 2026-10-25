# Livrable 4 : modèle de données NoSQL (MongoDB)

**Énoncé :** un schéma pour les données non structurées et semi-structurées (JSON, XML, binaire) ;
des stratégies pour les relations entre documents (imbrication, référencement, indexation) ;
l'intégration avec l'OLTP et l'OLAP.

| Fichier | Contenu |
|---|---|
| [`modele_nosql.svg`](modele_nosql.svg) | collections, imbrications, références, index, déploiement |
| [`collections_mongodb.js`](collections_mongodb.js) | script mongosh : collections, validation, index, sharding, documents types |

![Modèle NoSQL](modele_nosql.svg)

---

## 1. Rôle

MongoDB reçoit ce que le relationnel gère mal : structures **variables** (journaux, avis libres),
données **imbriquées** (une session et ses clics), volumes d'événements élevés, **pièces jointes**
(XML, PDF, images). C'est aussi le **magasin de features** du modèle de fraude (livrable 7).

Technologie : **MongoDB** — le document JSON est la forme naturelle des événements applicatifs, et
le schéma reste flexible tout en pouvant être validé là où c'est nécessaire.

## 2. Les collections

| Collection | Données de l'énoncé | Forme du document | Référence |
|---|---|---|---|
| `user_sessions` | flux de clics, sessions | session + `device {}` + `events []` imbriqués | `customer_id` → OLTP |
| `customer_feedback` | avis, enquêtes | document plat, schéma libre | `customer_id` → OLTP |
| `logs` | journaux d'erreurs et d'accès | plat, champs propres à chaque service | `transaction_id` (facultatif) |
| `ml_features` | caractéristiques d'apprentissage | `features {}` + étiquette `is_fraud` | `transaction_id` (index unique) |
| `dispute_evidence` | XML et binaire | champs extraits du XML + XML d'origine + adresse du fichier dans le stockage objet | `transaction_id`, `file.object_key` |
| `customer_profiles` | personnalisation (livrable 7) | un document par client : segment comportemental, recommandations | `_id` = `customer_id` |

## 3. Imbriquer ou référencer

La règle : **on modélise selon les requêtes**, pas selon les entités.

**Imbriquer** quand les données sont lues et écrites ensemble (appareil et clics d'une session),
que leur nombre est borné (le validateur limite `events` à 1 000) et qu'elles n'ont pas de vie
propre (variables d'une ligne de features).

**Référencer** quand la donnée maître vit ailleurs (client et transaction restent dans l'OLTP),
que la relation est non bornée (sessions d'un client) ou que l'objet est volumineux (fichier dans
le stockage objet, seule son adresse est conservée).

MongoDB ne joint pas PostgreSQL : la « jointure » est **logique** — on lit la transaction dans
l'OLTP, puis ses features par `transaction_id` (index unique). `$lookup` reste possible en interne,
mais pas dans les chemins critiques.

## 4. JSON, XML et binaire

| Format | Stratégie |
|---|---|
| **JSON** | format natif (BSON) : événements, sessions, avis, features |
| **XML** (messages bancaires de rétrofacturation) | champs utiles **extraits** en sous-document `bank_message` (requêtables, indexables) ; message d'origine **conservé** dans `raw_xml` pour l'audit |
| **Binaire** (PDF, images) | **stockage objet chiffré** (S3) : ≈ 27 To par an (livrable 0 §3.4), bien plus économique qu'une base ; le document porte l'adresse (`object_key`), l'empreinte SHA-256 (preuve que la pièce n'a pas été modifiée), la taille et le type. GridFS reste possible pour de faibles volumes |

## 5. Schéma flexible, mais contrôlé

- **Validation `$jsonSchema`** là où un traitement en dépend : `ml_features` (`transaction_id`,
  `features` avec `amt`, `geo_distance_km`, `velocity_1h`, `computed_at`), `user_sessions`,
  `dispute_evidence`.
- **Schéma libre** pour `logs` et `customer_feedback` : un nouveau service ajoute ses champs sans
  migration.
- **Évolution des documents** : un champ nouveau s'ajoute sans migration ; un changement de sens
  d'un champ passe par un champ `schema_version`, que les lecteurs testent.
- **Étiquette tardive** : `is_fraud` vaut `null` à la création, renseignée quand la fraude est
  confirmée (rétrofacturation, enquête), parfois des semaines plus tard.

## 6. Index

| Index | Requête servie |
|---|---|
| `ml_features {transaction_id: 1}` **unique** | features d'une transaction (scoring) |
| `ml_features {computed_at: 1}` **TTL** | fenêtre récente pour la dérive et l'entraînement ; purge à 180 jours |
| `user_sessions {started_at: 1}` **TTL** | purge à 90 jours (agrégées avant dans `customer_profiles` et Snowflake) |
| `user_sessions {customer_id: 1, started_at: -1}` | parcours d'un client |
| `customer_feedback {customer_id: 1, created_at: -1}` | avis d'un client |
| `dispute_evidence {transaction_id: 1}` | pièces d'un litige |
| `logs {ts: 1}` **TTL** | purge automatique à 30 jours (minimisation RGPD) |

Un index par motif de requête fréquent, et pas plus : chaque index ralentit les écritures.

## 7. Volume et montée en charge

Chiffres du [livrable 0](../00_cahier_des_charges/README.md) §3.4 : **≈ 570 Go par jour**,
**≈ 40 To** gardés à chaud (≈ 52 To avec les index), jusqu'à **≈ 26 000 écritures par seconde**
au pic. Un seul serveur ne tient pas ce volume : MongoDB est **réparti dès le départ** (sharding).

| Collection | Clé de répartition | Conservation à chaud | Volume chaud |
|---|---|---|---|
| `user_sessions` | `customer_id` haché | 90 jours (TTL) | 10 To |
| `logs` | `_id` haché | 30 jours (TTL) | 11 To |
| `ml_features` | `transaction_id` haché | 180 jours (TTL) | 14 To |
| `dispute_evidence` | `transaction_id` haché | 5 ans (fichiers dans le stockage objet) | 2,7 To |
| `customer_profiles` | `_id` haché, ou région + `_id` (voir ci-dessous) | durée de la relation | 1,5 To |
| `customer_feedback` | `customer_id` haché | 3 ans | 0,3 To |

- **≈ 16 shards**, chacun un replica set de 3 nœuds (≈ 3,3 To par shard) : un serveur en panne
  n'arrête rien, une écriture n'est confirmée que lorsque la majorité des nœuds l'a reçue
  (`writeConcern: majority`).
- **Clé hachée** : des identifiants qui se suivent ne partent pas tous sur le même serveur ; les
  écritures se répartissent uniformément.
- **Ajouter un shard** quand un shard dépasse 70 % de stockage ou d'écritures : MongoDB déplace
  lui-même une partie des données vers le nouveau (rééquilibrage), sans arrêt.
- **Résidence des données (RGPD)** : les profils des clients européens restent sur des shards
  situés dans l'Union européenne (*zone sharding*), avec une clé qui commence par la région.
- **Scénario ×10** : ≈ 400 To chauds, soit ≈ 120 shards ; ou, plus économique, des durées de
  conservation à chaud raccourcies (sessions 30 jours, features 90 jours) et le reste en archive
  dans Snowflake : ≈ 50 shards.
- **Transactions multi-documents** disponibles sur replica set et cluster réparti.
- **Accès** : un utilisateur par usage ; l'ingestion écrit, le modèle ne fait que lire.

## 8. Intégration OLTP et OLAP

| Flux | Mécanisme |
|---|---|
| Application → MongoDB | événements publiés dans Kafka, chargés par un connecteur en écritures idempotentes (upsert sur l'identifiant d'événement ou `transaction_id`) en moins de 5 secondes |
| MongoDB ↔ OLTP | références logiques `customer_id`, `transaction_id` ; aucune donnée maître dupliquée |
| MongoDB → décision en ligne | l'API de paiement lit le profil du client (`customer_profiles`) pendant le paiement (livrable 7) |
| MongoDB → OLAP | les features servent à l'entraînement et au rattrapage ; les scores vont dans `ML.FRAUD_SCORES`, que dbt rattache aux faits |
| Réconciliation | une transaction se retrouve dans les trois systèmes par `transaction_id` (livrable 8) |

## 9. Dans la maquette

MongoDB 7 sur **un seul nœud**
([`maquette/init/mongo/01-users-and-indexes.js`](../maquette/init/mongo/01-users-and-indexes.js)) :
collections `ml_features`, `user_sessions`, `customer_feedback` et `logs`, alimentées depuis Kafka,
mêmes index, validation de `ml_features`, deux utilisateurs (`el_loader` en écriture, `ml_reader`
en lecture). Le script cible complet ([`collections_mongodb.js`](collections_mongodb.js)) a été
exécuté sans erreur dans un MongoDB 7 jetable.

Non reproduits : `dispute_evidence` et le stockage objet, `customer_profiles`, la validation de
`user_sessions`, les TTL de 90 et 180 jours, le replica set, le sharding (plusieurs serveurs
nécessaires). L'étiquette `is_fraud` y est fournie dès l'émission de l'événement, pour pouvoir
entraîner sans attendre.
