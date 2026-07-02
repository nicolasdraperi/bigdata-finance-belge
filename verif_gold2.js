const g = db.getSiblingDB('bce').hotel_gold;

print("=== Hôtel avec CA (schéma full) ===");
const avecCA = g.findOne({ "years.chiffre_affaires": { $gt: 0 } });
if (avecCA) {
  print("Entreprise : " + avecCA.enterprise_number + " | schema : " + avecCA.schema_type);
  avecCA.years.forEach(y => print("  " + y.year + " | CA=" + y.chiffre_affaires + " | net=" + y.resultat_net + " | marge_nette=" + y.ratios.marge_nette_pct + "% | ROE=" + y.ratios.roe_pct + "%"));
} else {
  print("Aucun hôtel avec CA > 0 (tous en abrégé ?)");
}

print("\n=== Répartition schema_type ===");
g.aggregate([{ $group: { _id: "$schema_type", n: { $sum: 1 } } }]).forEach(r => print("  " + r._id + " : " + r.n));

print("\n=== Couverture CA ===");
print("  avec CA>0 : " + g.countDocuments({ "years.chiffre_affaires": { $gt: 0 } }));
print("  total : " + g.countDocuments());

print("\n=== Ratios aberrants (ROE hors [-1000, 1000]) ===");
const aberrants = g.countDocuments({ "years.ratios.roe_pct": { $gt: 1000 } });
print("  ROE > 1000% : " + aberrants);