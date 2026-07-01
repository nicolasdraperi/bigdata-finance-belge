const s = db.getSiblingDB('bce').state_db;
print("Remis en pending : " + s.updateMany({ status: "in_progress" }, { $set: { status: "pending" } }).modifiedCount);