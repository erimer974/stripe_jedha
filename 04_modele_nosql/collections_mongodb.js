// ============================================================
// Livrable 4 — Modèle NoSQL (MongoDB), architecture cible
// Étude de cas Stripe · base stripe_nosql
//
// Script mongosh : collections, validation de schéma, index, documents types.
//   mongosh "<URI administrateur>" --file collections_mongodb.js
//
// La maquette crée le sous-ensemble utilisé par la démonstration
// (4 collections, mêmes index, validation de ml_features seulement, sans dispute_evidence ni sharding) :
// maquette/init/mongo/01-users-and-indexes.js
// ============================================================

const nosql = db.getSiblingDB("stripe_nosql");

// ------------------------------------------------------------
// 1. ml_features : un document de features par transaction
//    Schéma souple mais contrôlé : les champs indispensables au modèle sont obligatoires,
//    de nouvelles variables peuvent s'ajouter dans `features` sans migration.
// ------------------------------------------------------------
nosql.createCollection("ml_features", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["transaction_id", "features", "computed_at"],
      properties: {
        transaction_id: { bsonType: ["int", "long"], description: "référence vers oltp.transactions" },
        features: {
          bsonType: "object",
          required: ["amt", "geo_distance_km", "velocity_1h"],
        },
        is_fraud: { bsonType: ["bool", "null"], description: "étiquette confirmée (rétrofacturation, enquête)" },
        computed_at: { bsonType: "date" },
      },
    },
  },
});

// ------------------------------------------------------------
// 2. user_sessions : session + appareil + clics IMBRIQUÉS (embedding)
//    Les clics sont toujours lus avec leur session et leur nombre est borné.
// ------------------------------------------------------------
nosql.createCollection("user_sessions", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["customer_id", "started_at", "events"],
      properties: {
        customer_id: { bsonType: ["int", "long"], description: "référence vers oltp.customers" },
        started_at: { bsonType: "date" },
        device: { bsonType: "object" },
        events: { bsonType: "array", maxItems: 1000 },   // borne : empêche un document de grossir sans fin
      },
    },
  },
});

// ------------------------------------------------------------
// 3. customer_feedback et logs : documents plats, schéma libre
// ------------------------------------------------------------
nosql.createCollection("customer_feedback");
nosql.createCollection("logs");

// ------------------------------------------------------------
// 4. dispute_evidence : pièces de litige (cible uniquement)
//    - message bancaire XML : conservé tel quel (raw_xml) ET converti en champs
//      JSON requêtables (bank_message) à l'ingestion ;
//    - fichier binaire (PDF, image) : GridFS au-delà de 16 Mo, référencé par file_id.
// ------------------------------------------------------------
nosql.createCollection("dispute_evidence", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["transaction_id", "type", "submitted_at"],
      properties: {
        transaction_id: { bsonType: ["int", "long"] },
        type: { enum: ["receipt", "shipping_proof", "customer_communication", "bank_message"] },
        file_id: { bsonType: "objectId", description: "référence vers fs.files (GridFS)" },
        bank_message: { bsonType: "object" },
        raw_xml: { bsonType: "string" },
        submitted_at: { bsonType: "date" },
      },
    },
  },
});

// ------------------------------------------------------------
// 4 bis. customer_profiles : résultats de personnalisation (livrable 7, cible uniquement)
//    Un document par client, _id = customer_id : lecture directe par clé, en millisecondes,
//    par l'application. Réécrit en entier à chaque calcul (idempotent).
// ------------------------------------------------------------
nosql.createCollection("customer_profiles");

// ------------------------------------------------------------
// 5. Index : un par motif de requête fréquent, pas plus
// ------------------------------------------------------------
nosql.ml_features.createIndex({ transaction_id: 1 }, { unique: true });      // jointure logique avec l'OLTP
nosql.ml_features.createIndex({ computed_at: -1 });                           // fenêtres récentes (dérive, entraînement)
nosql.user_sessions.createIndex({ customer_id: 1, started_at: -1 });          // parcours d'un client
nosql.customer_feedback.createIndex({ customer_id: 1, created_at: -1 });      // avis d'un client
nosql.dispute_evidence.createIndex({ transaction_id: 1 });                    // pièces d'un litige
nosql.logs.createIndex({ ts: 1 }, { expireAfterSeconds: 60 * 60 * 24 * 30 }); // TTL : purge à 30 jours

// ------------------------------------------------------------
// 6. Utilisateurs au moindre privilège (mots de passe fournis par le coffre de secrets)
// ------------------------------------------------------------
// nosql.createUser({ user: "el_loader", pwd: passwordPrompt(), roles: [{ role: "readWrite", db: "stripe_nosql" }] });
// nosql.createUser({ user: "ml_reader", pwd: passwordPrompt(), roles: [{ role: "read", db: "stripe_nosql" }] });

// ------------------------------------------------------------
// 7. Sharding (cluster de production, à exécuter via mongos)
//    Clé hachée : répartition uniforme des écritures, pas de point chaud sur les
//    identifiants croissants.
// ------------------------------------------------------------
// sh.enableSharding("stripe_nosql");
// sh.shardCollection("stripe_nosql.ml_features",       { transaction_id: "hashed" });
// sh.shardCollection("stripe_nosql.dispute_evidence",  { transaction_id: "hashed" });
// sh.shardCollection("stripe_nosql.user_sessions",     { customer_id: "hashed" });
// sh.shardCollection("stripe_nosql.customer_feedback", { customer_id: "hashed" });
// Remarque : un index unique n'est autorisé sur une collection shardée que s'il
// commence par la clé de sharding ; c'est le cas de ml_features (transaction_id).

// ------------------------------------------------------------
// 8. Documents types
// ------------------------------------------------------------
const now = new Date();

nosql.ml_features.insertOne({
  transaction_id: NumberLong("987654"),
  features: { amt: 240.5, geo_distance_km: 1200, velocity_1h: 7 },
  is_fraud: null,                        // inconnu tant qu'aucune rétrofacturation n'est confirmée
  computed_at: now,
});

nosql.user_sessions.insertOne({
  _id: "sess_3f9a1c2b7d10",
  customer_id: NumberLong("12345"),
  device: { type: "mobile", os: "iOS" },
  started_at: now,
  events: [
    { type: "click", page: "/checkout", ts: now },
    { type: "click", page: "/pay", ts: now },
  ],
});

nosql.customer_feedback.insertOne({
  _id: "5b1e0c8f2a",
  customer_id: NumberLong("12345"),
  rating: 4,
  text: "Paiement rapide",
  created_at: now,
});

nosql.logs.insertOne({
  _id: "a91f33c0d2",
  level: "ERROR",
  service: "payment-api",
  message: "payment declined by risk engine or bank gateway",
  transaction_id: NumberLong("987654"),
  ts: now,
});

nosql.customer_profiles.replaceOne(
  { _id: NumberLong("12345") },
  {
    behavior_segment: "mobile_frequent",
    recommendations: ["wallet", "one_click_checkout"],
    model_version: "3",
    computed_at: now,
  },
  { upsert: true },
);

nosql.dispute_evidence.insertOne({
  transaction_id: NumberLong("987654"),
  type: "bank_message",
  bank_message: { reason_code: "10.4", amount: 240.5, currency: "EUR", received_on: now },
  raw_xml: "<Chargeback><ReasonCode>10.4</ReasonCode><Amount ccy=\"EUR\">240.50</Amount></Chargeback>",
  submitted_at: now,
});
