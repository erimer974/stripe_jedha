# Livrable 2 : modèle de données OLTP (PostgreSQL)

**Énoncé :** un schéma normalisé pour un traitement transactionnel à haut volume, respectant les
propriétés ACID, avec réplication et basculement en temps réel.

| Fichier | Contenu |
|---|---|
| [`erd_oltp.svg`](erd_oltp.svg) | diagramme entité-relation (patte-d'oie), index, propriétés |
| [`schema_oltp.sql`](schema_oltp.sql) | modèle physique : tables, contraintes, index, référentiels, notes de déploiement |

![Diagramme entité-relation OLTP](erd_oltp.svg)

---

## 1. Rôle

L'OLTP est la **source de vérité opérationnelle** : l'application y écrit chaque transaction au
moment où elle a lieu, en quelques millisecondes, sans jamais enregistrer un paiement incohérent.
Les requêtes lourdes sont déportées vers l'OLAP (livrable 3).

Technologie : **PostgreSQL** — ACID, écosystème de réplication mature, offre managée partout.

## 2. Du besoin au modèle (MERISE)

| Règle de gestion | Conséquence |
|---|---|
| une transaction concerne un seul client | `transactions.customer_id` → `customers` (1-N) |
| une transaction concerne un seul commerçant | `transactions.merchant_id` → `merchants` (1-N) |
| une transaction a un moyen de paiement et une devise | `payment_method_id`, `currency_code` |
| client et commerçant sont rattachés à un pays | `country_code` → `countries` |
| un taux de change existe par devise **et** par date | `exchange_rates`, clé composite |

Champs de l'énoncé couverts : identifiants, montant, devise, moyen de paiement, `created_at`,
`ip_geolocation`, `device_type`, `status`, `fraud_score`.

## 3. Normalisation (3NF)

Valeurs atomiques (1NF) ; la seule clé composite n'a qu'un attribut non clé dépendant des deux
parties (2NF) ; aucun attribut non clé ne dépend d'un autre : pays, devises et moyens de paiement
vivent dans leur propre table (3NF). Le prix, ce sont les jointures — acceptable ici car
l'application lit presque toujours par clé.

## 4. Intégrité et ACID

| Mécanisme | Exemple |
|---|---|
| clés étrangères | pas de transaction sans client existant |
| `CHECK` | `amount > 0` ; `status` ∈ {succeeded, failed, refunded} ; `fraud_score` ∈ [0,1] |
| `UNIQUE` | un email par client |
| `NOT NULL` | client, commerçant, moyen de paiement, devise, montant, statut |

*Atomicité* : un lot est validé entièrement ou pas du tout. *Cohérence* : contraintes vérifiées à
chaque validation. *Isolation* : l'extraction lit toutes les tables dans un même instantané
(`REPEATABLE READ`), donc jamais une transaction sans son client. *Durabilité* : la transaction
validée est écrite dans le journal des transactions (WAL), puis répliquée (§6).

**Aucun numéro de carte n'est stocké** : la carte reste tokenisée chez le prestataire, ce qui sort
l'OLTP du périmètre le plus exigeant de PCI-DSS (livrable 6).

## 5. Index

| Index | Question servie |
|---|---|
| `idx_tx_customer`, `idx_tx_merchant` | historique d'un client, d'un commerçant |
| `idx_tx_created` | transactions récentes ; extraction incrémentale |
| `idx_tx_unscored` (partiel, `WHERE fraud_score IS NULL`) | transactions en attente de score |
| `idx_cust_created`, `idx_merch_created` | extraction incrémentale des nouveaux clients/commerçants |

On n'indexe que les colonnes filtrées en permanence : un index accélère les lectures mais ralentit
chaque écriture.

## 6. Haute disponibilité et reprise

| Besoin | Dispositif |
|---|---|
| aucune perte de transaction validée | réplica **synchrone** |
| continuité en cas de panne | **bascule automatique** : le réplica est promu à la place du primaire |
| lectures sans charger le primaire | réplicas asynchrones ; l'extraction y est branchée |
| reprise après sinistre | sauvegarde quotidienne et archivage continu du journal des transactions (WAL), ce qui permet de restaurer la base à un instant précis ; copie chiffrée hors région |

## 7. Partitionnement

Quand le volume l'exige, `transactions` est **partitionnée par mois sur `created_at`** (DDL en fin
de [`schema_oltp.sql`](schema_oltp.sql)) : les requêtes récentes ne lisent que la partition
courante, et les mois anciens s'archivent ou se détachent sans bloquer le reste de la table.

## 8. Intégration OLAP et NoSQL

| Point | Choix |
|---|---|
| clé de réconciliation | `transaction_id` : clé primaire ici, dimension dégénérée dans l'étoile, champ référencé dans MongoDB |
| registre en ajout seul | une transaction n'est jamais modifiée, sauf `fraud_score` ; un remboursement est une nouvelle ligne — c'est ce qui rend l'extraction incrémentale fiable |
| extraction | incrémentale sur `created_at`, recouvrement de 10 min, dédoublonnage sur la clé (livrable 5) |
| minimisation RGPD | le rôle d'extraction ne peut pas lire `customers.email` |
| retour du score | le modèle réécrit `fraud_score`, et seulement cette colonne (livrable 6) |

## 9. Dans la maquette

La maquette exécute **exactement ce modèle**
([`maquette/init/oltp/01-schema.sql`](../maquette/init/oltp/01-schema.sql)) sur une instance
PostgreSQL 16. Réplication, bascule, sauvegardes et partitionnement ne sont pas reproduits
(plusieurs nœuds nécessaires) ; ils ne changent rien au modèle logique.
