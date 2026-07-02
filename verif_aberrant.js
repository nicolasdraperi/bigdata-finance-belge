const g = db.getSiblingDB('bce').hotel_gold;
const d = g.findOne({ "years.ratios.roe_pct": { $gt: 1000 } });
print("Entreprise : " + d.enterprise_number);
d.years.forEach(y => {
  if (y.ratios.roe_pct > 1000)
    print("  " + y.year + " | net=" + y.resultat_net + " | fonds_propres=" + y.fonds_propres + " | ROE=" + y.ratios.roe_pct + "%");
});