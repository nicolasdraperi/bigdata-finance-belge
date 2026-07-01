const s = db.getSiblingDB('bce').state_db;
print("Nombre d'hôtels ciblés (StateDB) : " + s.countDocuments());
print("\nRépartition par statut :");
s.aggregate([{ $group: { _id: "$status", n: { $sum: 1 } } }]).forEach(r => print("  " + r._id + " : " + r.n));
print("\nExemples :");
s.find().limit(5).forEach(d => print("  " + d.EnterpriseNumber + " (" + d.status + ")"));