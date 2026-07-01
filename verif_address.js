const c = db.getSiblingDB('bce').enterprise_silver;

const nonRego = c.countDocuments({ addresses: { $elemMatch: { TypeOfAddress: { $ne: 'REGO' } } } });
print("Entreprises avec adresse non-REGO restante : " + nonRego);

const d = c.findOne({ 'addresses.0': { $exists: true } });
if (d) {
  print("\nExemple " + d.EnterpriseNumber + " -> " + d.addresses.length + " adresse(s) :");
  d.addresses.forEach(a => print("  " + a.TypeOfAddress + " - " + a.MunicipalityFR));
}