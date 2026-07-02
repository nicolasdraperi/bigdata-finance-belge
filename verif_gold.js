const g = db.getSiblingDB('bce').hotel_gold;
print("Docs hotel_gold : " + g.countDocuments());
const d = g.findOne();
print("\nEntreprise : " + d.enterprise_number + " | schema : " + d.schema_type + " | années : " + d.years.length);
d.years.forEach(y => {
  print("\n--- " + y.year + " ---");
  print("  CA : " + y.chiffre_affaires + " | résultat net : " + y.resultat_net);
  print("  Ratios : marge_nette=" + y.ratios.marge_nette_pct + "% | ROE=" + y.ratios.roe_pct + "% | endettement=" + y.ratios.taux_endettement_pct + "%");
});