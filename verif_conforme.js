const s = db.getSiblingDB('bce').state_db;

print("=== Exemple avec fichiers ===");
const d = s.findOne({ status: "done", filings_count: { $gt: 0 } });
printjson({
  EnterpriseNumber: d.EnterpriseNumber,
  status: d.status,
  filings_count: d.filings_count,
  downloaded_refs: d.downloaded_refs
});

print("\n=== Statistiques ===");
print("Hôtels done : " + s.countDocuments({ status: "done" }));
print("  dont avec fichiers : " + s.countDocuments({ status: "done", filings_count: { $gt: 0 } }));
print("  dont sans dépôt NBB (0 fichier) : " + s.countDocuments({ status: "done", filings_count: 0 }));


const tot = s.aggregate([
  { $match: { status: "done" } },
  { $group: { _id: null, total: { $sum: "$filings_count" } } }
]).toArray();
print("\nTotal fichiers téléchargés : " + (tot[0] ? tot[0].total : 0));