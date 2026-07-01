const c = db.getSiblingDB('bce').enterprise_silver;
const d = c.findOne({ EnterpriseNumber: '0200.065.765' });
print("Status: " + d.Status + " -> StatusLabel: " + d.StatusLabel);
print("JuridicalForm: " + d.JuridicalForm + " -> JuridicalFormLabel: " + d.JuridicalFormLabel);
print("\nActivités avec labels :");
d.activities.forEach(a => print("  " + a.NaceCode + " (" + a.NaceVersion + ") -> " + a.NaceLabel));