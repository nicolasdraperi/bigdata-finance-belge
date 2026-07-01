const c = db.getSiblingDB('bce').enterprise_silver;

const d = c.findOne({ 'denominations.1': { $exists: true } });
if (d) {
  print("Exemple " + d.EnterpriseNumber + " -> " + d.denominations.length + " dénomination(s) :");
  d.denominations.forEach((x, i) => print("  [" + i + "] type=" + x.TypeOfDenomination + " : " + x.Denomination));
  print("\n=> La première doit être un '001' (nom officiel)");
}
const mauvais = c.countDocuments({
  $and: [
    { 'denominations.TypeOfDenomination': '001' },
    { 'denominations.0.TypeOfDenomination': { $ne: '001' } }
  ]
});
print("\nEntreprises avec un '001' pas en premier : " + mauvais + " (doit être 0)");