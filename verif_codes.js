const c = db.getSiblingDB('bce').code;
print("Catégories disponibles dans 'code' :");
c.distinct("Category").forEach(cat => print("  " + cat));

print("\nExemples pour quelques catégories clés :");
["Status", "JuridicalForm"].forEach(cat => {
  const ex = c.findOne({ Category: cat, Language: "FR" });
  if (ex) print("  " + cat + " -> code " + ex.Code + " = " + ex.Description);
});

print("\nCatégories liées au NACE :");
c.distinct("Category").filter(x => /nace/i.test(x)).forEach(cat => print("  " + cat));