from pymongo import MongoClient

MONGO_URI = "mongodb://root:root@mongo:27017/"
MONGO_DB  = "bce"

def _db():
    return MongoClient(MONGO_URI)[MONGO_DB]


def build_enterprise_finale(force=False):
    db = _db()
    src_count = db["enterprise"].estimated_document_count()
    fin_count = db["enterprise_finale"].estimated_document_count()
    if fin_count >= src_count and src_count > 0 and not force:
        print(f"enterprise_finale déjà fusionnée ({fin_count} docs) -> SKIP", flush=True)
        return {"enterprise_finale": fin_count, "status": "skipped"}

    pipeline = [
        {"$lookup": {"from": "denomination",  "localField": "EnterpriseNumber", "foreignField": "EntityNumber",     "as": "denominations"}},
        {"$lookup": {"from": "address",       "localField": "EnterpriseNumber", "foreignField": "EntityNumber",     "as": "addresses"}},
        {"$lookup": {"from": "activity",      "localField": "EnterpriseNumber", "foreignField": "EntityNumber",     "as": "activities"}},
        {"$lookup": {"from": "contact",       "localField": "EnterpriseNumber", "foreignField": "EntityNumber",     "as": "contacts"}},
        {"$lookup": {"from": "establishment", "localField": "EnterpriseNumber", "foreignField": "EnterpriseNumber", "as": "establishments"}},
        {"$merge": {"into": "enterprise_finale", "whenMatched": "replace", "whenNotMatched": "insert"}},
    ]
    db.enterprise.aggregate(pipeline, allowDiskUse=True)
    n = db["enterprise_finale"].estimated_document_count()
    print(f"enterprise_finale fusionnée : {n} docs -> LOADED", flush=True)
    return {"enterprise_finale": n, "status": "loaded"}

def silver_normalize_dates(force=False):
    db = _db()
    fin = db["enterprise_finale"].estimated_document_count()
    sil = db["enterprise_silver"].estimated_document_count()
    if sil >= fin and fin > 0 and not force:
        print(f"enterprise_silver déjà créée ({sil} docs) -> SKIP", flush=True)
        return {"enterprise_silver": sil, "status": "skipped"}

    pipeline = [
        {"$addFields": {
            "StartDate": {
                "$let": {
                    "vars": {"p": {"$split": ["$StartDate", "-"]}},
                    "in": {
                        "$cond": [
                            {"$eq": [{"$size": "$$p"}, 3]},
                            {"$concat": [
                                {"$arrayElemAt": ["$$p", 2]}, "-",
                                {"$arrayElemAt": ["$$p", 1]}, "-",
                                {"$arrayElemAt": ["$$p", 0]}
                            ]},
                            "$StartDate"
                        ]
                    }
                }
            }
        }},
        {"$merge": {"into": "enterprise_silver", "whenMatched": "replace", "whenNotMatched": "insert"}},
    ]
    db.enterprise_finale.aggregate(pipeline, allowDiskUse=True)
    n = db["enterprise_silver"].estimated_document_count()
    print(f"enterprise_silver dates normalisées : {n} docs -> LOADED", flush=True)
    return {"enterprise_silver": n, "step": "dates_normalized", "status": "loaded"}

def silver_dedup_activities(force=False):
    db = _db()
    already = db["enterprise_silver"].count_documents({"_silver_step2": True}, limit=1)
    if already and not force:
        print("dédup activités déjà faite -> SKIP", flush=True)
        return {"status": "skipped", "step": "dedup_activities"}

    pipeline = [
        {"$addFields": {
            "activities": {
                "$reduce": {
                    "input": "$activities",
                    "initialValue": [],
                    "in": {
                        "$cond": [
                            {"$in": [
                                {"$concat": [
                                    {"$ifNull": ["$$this.NaceCode", ""]}, "|",
                                    {"$ifNull": ["$$this.NaceVersion", ""]}, "|",
                                    {"$ifNull": ["$$this.Classification", ""]}
                                ]},
                                {"$map": {
                                    "input": "$$value",
                                    "as": "v",
                                    "in": {"$concat": [
                                        {"$ifNull": ["$$v.NaceCode", ""]}, "|",
                                        {"$ifNull": ["$$v.NaceVersion", ""]}, "|",
                                        {"$ifNull": ["$$v.Classification", ""]}
                                    ]}
                                }}
                            ]},
                            "$$value",
                            {"$concatArrays": ["$$value", ["$$this"]]}
                        ]
                    }
                }
            },
            "_silver_step2": True
        }},
        {"$merge": {"into": "enterprise_silver", "whenMatched": "replace", "whenNotMatched": "insert"}},
    ]
    db.enterprise_silver.aggregate(pipeline, allowDiskUse=True)
    print("dédup activités appliquée -> LOADED", flush=True)
    return {"status": "loaded", "step": "dedup_activities"}

def silver_address_rego(force=False):
    db = _db()
    already = db["enterprise_silver"].count_documents({"_silver_step3": True}, limit=1)
    if already and not force:
        print("filtre adresse REGO déjà fait -> SKIP", flush=True)
        return {"status": "skipped", "step": "address_rego"}

    pipeline = [
        {"$addFields": {
            "addresses": {
                "$filter": {
                    "input": "$addresses",
                    "as": "a",
                    "cond": {"$eq": ["$$a.TypeOfAddress", "REGO"]}
                }
            },
            "_silver_step3": True
        }},
        {"$merge": {"into": "enterprise_silver", "whenMatched": "replace", "whenNotMatched": "insert"}},
    ]
    db.enterprise_silver.aggregate(pipeline, allowDiskUse=True)
    print("filtre adresse REGO appliqué -> LOADED", flush=True)
    return {"status": "loaded", "step": "address_rego"}

def silver_denomination_principale(force=False):
    db = _db()
    already = db["enterprise_silver"].count_documents({"_silver_step4": True}, limit=1)
    if already and not force:
        print("dénomination principale déjà faite -> SKIP", flush=True)
        return {"status": "skipped", "step": "denomination_principale"}

    pipeline = [
        {"$addFields": {
            "denominations": {
                "$concatArrays": [
                    {"$filter": {"input": "$denominations", "as": "d",
                                 "cond": {"$eq": ["$$d.TypeOfDenomination", "001"]}}},
                    {"$filter": {"input": "$denominations", "as": "d",
                                 "cond": {"$ne": ["$$d.TypeOfDenomination", "001"]}}}
                ]
            },
            "_silver_step4": True
        }},
        {"$merge": {"into": "enterprise_silver", "whenMatched": "replace", "whenNotMatched": "insert"}},
    ]
    db.enterprise_silver.aggregate(pipeline, allowDiskUse=True)
    print("dénomination principale appliquée -> LOADED", flush=True)
    return {"status": "loaded", "step": "denomination_principale"}

def silver_decode_labels(force=False, batch_size=5000):
    db = _db()
    already = db["enterprise_silver"].count_documents({"_silver_step5": True}, limit=1)
    if already and not force:
        print("décodage labels déjà fait -> SKIP", flush=True)
        return {"status": "skipped", "step": "decode_labels"}

    code_map = {}
    for c in db.code.find({"Language": "FR"}, {"Category": 1, "Code": 1, "Description": 1, "_id": 0}):
        code_map[(c["Category"], c["Code"])] = c.get("Description", "")

    def label(cat, code):
        return code_map.get((cat, code), "")


    coll = db.enterprise_silver
    total = 0
    ops = []
    from pymongo import UpdateOne
    for doc in coll.find({}, {"Status": 1, "JuridicalForm": 1, "activities": 1}):
        set_fields = {
            "StatusLabel":        label("Status", doc.get("Status", "")),
            "JuridicalFormLabel": label("JuridicalForm", doc.get("JuridicalForm", "")),
            "_silver_step5":      True,
        }
        acts = doc.get("activities", [])
        for a in acts:
            cat = "Nace" + str(a.get("NaceVersion", ""))
            a["NaceLabel"] = label(cat, a.get("NaceCode", ""))
        set_fields["activities"] = acts

        ops.append(UpdateOne({"_id": doc["_id"]}, {"$set": set_fields}))
        if len(ops) >= batch_size:
            coll.bulk_write(ops, ordered=False); total += len(ops); ops = []
    if ops:
        coll.bulk_write(ops, ordered=False); total += len(ops)

    print(f"décodage labels appliqué sur {total} docs -> LOADED", flush=True)
    return {"status": "loaded", "step": "decode_labels", "docs": total}

# -- partie 2 hotelerie --

NACE_HOTELLERIE = ["55100", "55201", "55202", "55203", "55204",
                   "55209", "55300", "55400", "55900"]
JURIDICAL_FORMS_EXCLUS = ["110", "114", "116", "117",
                          "301", "302", "303",
                          "310", "320", "330", "340", "350",
                          "400", "411", "412", "413", "414", "415",
                          "416", "417", "418", "419", "420"]

def target_hotellerie(force=False):
    db = _db()
    state = db["state_db"]

    # skip si déjà peuplée
    if state.estimated_document_count() > 0 and not force:
        n = state.estimated_document_count()
        print(f"StateDB déjà peuplée ({n} entreprises) -> SKIP", flush=True)
        return {"state_db": n, "status": "skipped"}

    query = {
        "Status": "AC",
        "TypeOfEnterprise": "2",
        "JuridicalForm": {"$nin": JURIDICAL_FORMS_EXCLUS},
        "activities": {
            "$elemMatch": {
                "NaceCode": {"$in": NACE_HOTELLERIE},
                "Classification": "MAIN"
            }
        }
    }

    from pymongo import UpdateOne
    ops, total = [], 0
    for doc in db.enterprise_silver.find(query, {"EnterpriseNumber": 1}):
        num = doc["EnterpriseNumber"]
        ops.append(UpdateOne(
            {"_id": num},
            {"$setOnInsert": {
                "EnterpriseNumber": num,
                "status": "pending",
                "filings_count": 0,
                "downloaded_refs": []
            }},
            upsert=True
        ))
        if len(ops) >= 5000:
            state.bulk_write(ops, ordered=False); total += len(ops); ops = []
    if ops:
        state.bulk_write(ops, ordered=False); total += len(ops)

    n = state.estimated_document_count()
    print(f"StateDB peuplée : {n} hôtels en pending -> LOADED", flush=True)
    return {"state_db": n, "status": "loaded"}

def scrape_hotels_nbb(batch=200, workers=6, wait_429=60):
    """Scrape les hôtels pending EN PARALLÈLE via Tor (round-robin). Reprise via StateDB."""
    import bce_ingestion as ing
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from pymongo import MongoClient
    db = _db()
    state = db["state_db"]
    total = {"done": 0, "err": 0, "r429": 0}

    def _traiter(num):
        st = MongoClient(MONGO_URI)[MONGO_DB]["state_db"]   # 1 client par thread
        st.update_one({"_id": num}, {"$set": {"status": "in_progress"}})
        try:
            bilan = ing.ingest_cbso(num)
            st.update_one({"_id": num}, {"$set": {
                "status": "done",
                "filings_count": bilan["filings_count"],
                "downloaded_refs": bilan["refs"]}})
            return "done"
        except ing.RateLimited:
            ing.renew_tor_identity()
            st.update_one({"_id": num}, {"$set": {"status": "pending"}})
            return "r429"
        except Exception as e:
            st.update_one({"_id": num}, {"$set": {"status": "pending",
                                                  "last_error": str(e)[:200]}})
            return "err"

    while True:
        lot = list(state.find({"status": "pending"}, {"EnterpriseNumber": 1}).limit(batch))
        if not lot:
            print("Plus aucun hôtel pending -> terminé", flush=True)
            break
        nums = [d["EnterpriseNumber"] for d in lot]
        with ThreadPoolExecutor(max_workers=workers) as ex:
            for f in as_completed({ex.submit(_traiter, n) for n in nums}):
                res = f.result()
                key = "done" if res == "done" else ("r429" if res == "r429" else "err")
                total[key] += 1
        restants = state.count_documents({"status": "pending"})
        print(f"done: {total['done']} | 429: {total['r429']} | err: {total['err']} | pending: {restants}", flush=True)

    return total

def build_hotels_collection():
    """collection 'hotels' pour recherche rapide."""
    db = _db()
    nums = [d["_id"] for d in db.state_db.find({}, {"_id": 1})]
    print(f"{len(nums)} hôtels à copier", flush=True)

    from pymongo import UpdateOne
    ops = []
    cursor = db.enterprise_silver.find(
        {"EnterpriseNumber": {"$in": nums}},
        {"EnterpriseNumber": 1, "denominations": 1, "StatusLabel": 1, "JuridicalFormLabel": 1, "_id": 0}
    )
    for silver in cursor:
        num = silver["EnterpriseNumber"]
        denoms = silver.get("denominations", [])
        nom = denoms[0]["Denomination"] if denoms else "(sans nom)"
        ops.append(UpdateOne(
            {"_id": num},
            {"$set": {
                "enterprise_number": num,
                "nom": nom,
                "nom_lower": nom.lower(),
                "status": silver.get("StatusLabel", ""),
                "forme": silver.get("JuridicalFormLabel", ""),
            }},
            upsert=True))
        if len(ops) >= 1000:
            db.hotels.bulk_write(ops, ordered=False); ops = []
    if ops:
        db.hotels.bulk_write(ops, ordered=False)

    db.hotels.create_index("nom_lower")
    n = db.hotels.estimated_document_count()
    print(f"Collection hotels créée : {n} hôtels -> LOADED", flush=True)
    return {"hotels": n}