// ============================================================
// STOCKAGE NoSQL — MongoDB (sous-ensemble du modèle cible 04_modele_nosql/collections_mongodb.js)
// Utilisateurs au moindre privilège, validation de schéma souple, index.
// Exécuté une seule fois, à la création du volume.
// ============================================================
const svcPassword = process.env.SERVICE_PASSWORD;
const nosql = db.getSiblingDB("stripe_nosql");

// ---- Utilisateurs ----
nosql.createUser({ user: "el_loader", pwd: svcPassword, roles: [{ role: "readWrite", db: "stripe_nosql" }] }); // ingestion
nosql.createUser({ user: "ml_reader", pwd: svcPassword, roles: [{ role: "read", db: "stripe_nosql" }] });      // modèle ML

// ---- Schéma flexible mais contrôlé : les champs indispensables au ML sont obligatoires ----
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
        computed_at: { bsonType: "date" },
      },
    },
  },
});

// ---- Index ----
nosql.ml_features.createIndex({ transaction_id: 1 }, { unique: true });   // jointure logique avec l'OLTP
nosql.ml_features.createIndex({ computed_at: -1 });                        // fenêtres récentes (dérive)
nosql.user_sessions.createIndex({ customer_id: 1, started_at: -1 });       // parcours d'un client
nosql.customer_feedback.createIndex({ customer_id: 1, created_at: -1 });  // avis d'un client, du plus récent au plus ancien
nosql.logs.createIndex({ ts: 1 }, { expireAfterSeconds: 60 * 60 * 24 * 30 }); // TTL : purge des logs à 30 jours
