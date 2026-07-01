const s = db.getSiblingDB('bce').state_db;
const r = s.updateMany({ status: "done" }, { $set: { status: "pending" } });
print("Hôtels remis en pending : " + r.modifiedCount);