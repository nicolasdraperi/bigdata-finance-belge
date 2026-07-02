const c = db.getSiblingDB('bce').notaire_cache;
const r = c.deleteMany({ _id: "0878065378" });
print("Supprimé du cache Mongo : " + r.deletedCount);