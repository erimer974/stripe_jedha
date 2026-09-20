// ============================================================
// Requêtes NoSQL — requêtes N1 à N8 du livrable 8 (MongoDB, rôle ml_reader)
// Copie exécutable de 08_requetes/requetes_mongodb.js ; N8 est ignorée ici
// (collection dispute_evidence absente de la maquette).
//   docker compose exec mongo mongosh "mongodb://ml_reader:<SERVICE_PASSWORD>@localhost/stripe_nosql?authSource=stripe_nosql" --quiet --file /queries/03_mongodb.js
// ============================================================

// N1 à N7 portent sur les collections présentes dans la maquette ;
// N8 porte sur dispute_evidence (architecture cible uniquement).
// ============================================================

// N1 — Features de risque des 3 dernières transactions (référence OLTP par transaction_id)
printjson(db.ml_features.find({}, { _id: 0, transaction_id: 1, features: 1, computed_at: 1 })
  .sort({ computed_at: -1 }).limit(3).toArray());

// N2 — Parcours client : sessions et clics par type d'appareil (tableau imbriqué déplié)
printjson(db.user_sessions.aggregate([
  { $unwind: "$events" },
  { $group: { _id: "$device.type", sessions: { $addToSet: "$_id" }, clicks: { $sum: 1 } } },
  { $project: { _id: 0, device: "$_id", nb_sessions: { $size: "$sessions" }, nb_clicks: "$clicks" } },
]).toArray());

// N3 — Satisfaction : note moyenne et volume d'avis
printjson(db.customer_feedback.aggregate([
  { $group: { _id: null, note_moyenne: { $avg: "$rating" }, nb_avis: { $sum: 1 } } },
  { $project: { _id: 0 } },
]).toArray());

// N4 — Erreurs de l'API de paiement sur la dernière heure (journaux purgés automatiquement par TTL)
printjson(db.logs.countDocuments({ level: "ERROR", ts: { $gte: new Date(Date.now() - 3600 * 1000) } }));

// N5 — Pages les plus visitées dans les parcours (flux de clics)
printjson(db.user_sessions.aggregate([
  { $unwind: "$events" },
  { $match: { "events.type": "click" } },
  { $group: { _id: "$events.page", nb_clics: { $sum: 1 } } },
  { $sort: { nb_clics: -1 } },
  { $limit: 10 },
]).toArray());

// N6 — Satisfaction mensuelle : note moyenne et avis négatifs (note ≤ 2)
printjson(db.customer_feedback.aggregate([
  { $group: {
      _id: { $dateToString: { format: "%Y-%m", date: "$created_at" } },
      note_moyenne: { $avg: "$rating" },
      nb_avis: { $sum: 1 },
      nb_negatifs: { $sum: { $cond: [{ $lte: ["$rating", 2] }, 1, 0] } },
  } },
  { $sort: { _id: 1 } },
]).toArray());

// N7 — Erreurs par service sur 24 heures, avec les transactions concernées (supervision)
printjson(db.logs.aggregate([
  { $match: { level: "ERROR", ts: { $gte: new Date(Date.now() - 24 * 3600 * 1000) } } },
  { $group: { _id: "$service", nb_erreurs: { $sum: 1 }, transactions: { $addToSet: "$transaction_id" } } },
  { $project: { _id: 0, service: "$_id", nb_erreurs: 1, nb_transactions: { $size: "$transactions" } } },
  { $sort: { nb_erreurs: -1 } },
]).toArray());

// N8 — (cible) Litiges par motif bancaire : champs extraits du XML, requêtables sans relire le message
if (db.getCollectionNames().includes("dispute_evidence")) {
  printjson(db.dispute_evidence.aggregate([
    { $match: { type: "bank_message" } },
    { $group: { _id: "$bank_message.reason_code", nb_litiges: { $sum: 1 }, montant_total: { $sum: "$bank_message.amount" } } },
    { $sort: { nb_litiges: -1 } },
  ]).toArray());
}
