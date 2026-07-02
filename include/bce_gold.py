from pymongo import MongoClient, UpdateOne

MONGO_URI = "mongodb://root:root@mongo:27017/"
MONGO_DB = "bce"
HDFS_BASE = "hdfs://namenode:9000/data/raw/cbso"

PCMN_SIMPLE = {
    "70": "chiffre_affaires",
    "60": "achats",
    "71": "variation_stocks",
    "9901": "ebit",
    "9904": "resultat_net",
    "100": "capital_souscrit",
    "10/15": "fonds_propres",
    "9900": "resultat_exploitation",
}

PCMN_SOMMES = {
    "tresorerie": ["54", "55", "54/58"],
    "dettes_financieres": ["17", "43"],
}


def _ratios(f):
    ca = f.get("chiffre_affaires", 0.0)
    net = f.get("resultat_net", 0.0)
    fp = f.get("fonds_propres", 0.0)
    tre = f.get("tresorerie", 0.0)
    det = f.get("dettes_financieres", 0.0)
    ach = f.get("achats", 0.0)
    vs = f.get("variation_stocks", 0.0)

    def pct(num, den):
        return round(num / den * 100, 2) if den else None

    def rat(num, den):
        return round(num / den, 4) if den else None

    return {
        "marge_brute": round(ca - ach + vs, 2),
        "marge_nette_pct": pct(net, ca),
        "roe_pct": pct(net, fp),
        "ratio_liquidite": rat(tre, det),
        "taux_endettement_pct": pct(det, fp),
    }


def _parse_csv_content(lines):
    import csv as _csv

    raw = {}
    meta = {}

    for row in _csv.reader(lines):
        if len(row) != 2:
            continue

        code, val = row[0].strip(), row[1].strip()

        if code == "Accounting period end date":
            meta["year"] = val[:4]
        elif code == "Model code":
            meta["model"] = val
        else:
            try:
                raw[code] = float(val.replace(",", "."))
            except ValueError:
                pass

    f = {
        champ: raw.get(code, 0.0)
        for code, champ in PCMN_SIMPLE.items()
    }

    for champ, codes in PCMN_SOMMES.items():
        f[champ] = sum(raw.get(code, 0.0) for code in codes)

    return meta, f


def _schema_type(model):
    m = (model or "").lower()

    if "micro" in m or m.startswith("m9"):
        return "micro"

    if "f" in m:
        return "full"

    return "abrege"


def build_gold(limit=None):
    from pyspark.sql import SparkSession
    from datetime import datetime

    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]

    print("Gold : lecture directe des CSV disponibles dans HDFS", flush=True)

    spark = (
        SparkSession.builder
        .master("spark://spark-master:7077")
        .appName("bce_gold")
        .getOrCreate()
    )

    rdd = spark.sparkContext.wholeTextFiles(f"{HDFS_BASE}/*/csv/*.csv")
    fichiers = rdd.collect()
    spark.stop()

    if limit:
        fichiers = fichiers[:limit]

    print(f"Gold : {len(fichiers)} fichiers CSV détectés", flush=True)

    resultats = []

    for path, content in fichiers:
        try:
            num = path.split("/cbso/")[1].split("/")[0]
        except IndexError:
            continue

        meta, f = _parse_csv_content(content.splitlines())
        year = meta.get("year")

        if not year:
            continue

        f["year"] = int(year)
        f["ratios"] = _ratios(f)
        f["_model"] = meta.get("model", "")

        resultats.append((num, f))

    par_ent = {}

    for num, f in resultats:
        model = f.pop("_model", "")
        par_ent.setdefault(num, {"years": [], "model": model})
        par_ent[num]["years"].append(f)

    ops = []

    for num, data in par_ent.items():
        years = sorted(data["years"], key=lambda y: y["year"], reverse=True)

        ops.append(
            UpdateOne(
                {"_id": num},
                {
                    "$set": {
                        "enterprise_number": num,
                        "years": years,
                        "schema_type": _schema_type(data["model"]),
                        "last_updated": datetime.utcnow(),
                    }
                },
                upsert=True,
            )
        )

    if ops:
        db.hotel_gold.bulk_write(ops, ordered=False)

    n = db.hotel_gold.estimated_document_count()
    client.close()

    print(f"hotel_gold peuplée : {n} entreprises -> LOADED", flush=True)

    return {
        "hotel_gold": n,
        "traites": len(par_ent),
        "csv_lus": len(fichiers),
    }


def detecter_nouveaux_depots(limit=None):
    import bce_ingestion as ing
    import re

    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    a_retraiter = []

    cur = db.state_db.find(
        {"status": "done"},
        {"_id": 1, "downloaded_refs": 1}
    )

    if limit:
        cur = cur.limit(limit)

    for doc in cur:
        num = doc["_id"]
        numero = num.replace(".", "")

        try:
            deps_actuels = ing.comptes_retenus(numero)
            annees_actuelles = set(deps_actuels.keys())
        except Exception:
            continue

        annees_connues = set()

        for ref in doc.get("downloaded_refs", []):
            m = re.search(r"(\d{4})", ref)
            if m:
                annees_connues.add(int(m.group(1)))

        if annees_actuelles - annees_connues:
            a_retraiter.append(num)

    client.close()

    print(f"{len(a_retraiter)} entreprises avec de nouveaux dépôts", flush=True)

    return a_retraiter


def gold_incremental(nums):
    if not nums:
        print("Aucune entreprise à retraiter -> SKIP", flush=True)
        return {"retraites": 0}

    import bce_ingestion as ing

    for num in nums:
        try:
            ing.ingest_cbso(num)
        except Exception as e:
            print(f"  {num}: erreur scrape ({str(e)[:60]})", flush=True)

    return build_gold()