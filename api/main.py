"""API FastAPI : expose les données Silver + Gold, et streame les statuts notaire (SSE)."""
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sse_starlette.sse import EventSourceResponse
from pymongo import MongoClient
from hdfs import InsecureClient
import re, json, asyncio, time
import requests as _rq
from bs4 import BeautifulSoup
import os
MONGO_URI = "mongodb://root:root@mongo:27017/"
db = MongoClient(MONGO_URI)["bce"]
SEARCH_SCOPE = os.getenv("SEARCH_SCOPE", "hotels")
HDFS_API    = InsecureClient("http://namenode:9870", user="root")
AIRFLOW_API = "http://airflow-webserver:8080/api/v1"
AIRFLOW_AUTH = ("airflow", "airflow")

app = FastAPI(title="BCE Hôtellerie API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

def _norm(num: str) -> str:
    d = re.sub(r"\D", "", num)
    return f"{d[0:4]}.{d[4:7]}.{d[7:10]}" if len(d) == 10 else num

@app.get("/")
def root():
    return {"service": "BCE Hôtellerie API", "status": "ok"}

@app.get("/search")
def search(q: str, scope: str = "hotels", limit: int = 20):
    q = q.strip()
    if not q:
        return {"results": []}
    digits = re.sub(r"\D", "", q)

    if scope == "hotels":
        if 4 <= len(digits) <= 10:
            query = {"_id": _norm(digits.zfill(10))} if len(digits) == 10 else {
                "_id": {"$regex": "^" + re.escape(digits)}}
        else:
            query = {"nom_lower": {"$regex": re.escape(q.lower())}}
        results = []
        for doc in db.hotels.find(query).limit(limit):
            results.append({
                "enterprise_number": doc["enterprise_number"],
                "nom": doc["nom"],
                "status": doc.get("status", ""),
                "forme": doc.get("forme", ""),
            })
        return {"results": results, "count": len(results), "scope": "hotels"}
    if 4 <= len(digits) <= 10:
        num = _norm(digits.zfill(10)) if len(digits) == 10 else None
        query = {"EnterpriseNumber": num} if num else {
            "EnterpriseNumber": {"$regex": "^" + re.escape(digits)}}
    else:
        query = {"denominations.Denomination": {"$regex": re.escape(q), "$options": "i"}}
    results = []
    for doc in db.enterprise_silver.find(query, {
        "EnterpriseNumber": 1, "denominations": 1, "StatusLabel": 1, "JuridicalFormLabel": 1
    }).limit(limit):
        denoms = doc.get("denominations", [])
        nom = denoms[0]["Denomination"] if denoms else "(sans nom)"
        results.append({
            "enterprise_number": doc["EnterpriseNumber"],
            "nom": nom,
            "status": doc.get("StatusLabel", ""),
            "forme": doc.get("JuridicalFormLabel", ""),
        })
    return {"results": results, "count": len(results), "scope": "all"}

@app.get("/enterprise/{num}")
def enterprise(num: str):
    num_fmt = _norm(num)
    num_raw = re.sub(r"\D", "", num)
    silver = db.enterprise_silver.find_one({"EnterpriseNumber": num_fmt}, {"_id": 0})
    enriched = False
    if not silver:
        num_kbo = num_raw.lstrip("0")
        scraped = _scrape_entreprise_kbo(num_kbo)
        if not scraped:
            raise HTTPException(404, f"Entreprise {num_fmt} introuvable (ni en base, ni sur kbopub)")
        db.enterprise_silver.insert_one(dict(scraped))
        silver = scraped
        enriched = True

    denoms = silver.get("denominations", [])
    addrs = silver.get("addresses", [])
    acts = silver.get("activities", [])
    fiche = {
        "enterprise_number": silver["EnterpriseNumber"],
        "nom": denoms[0]["Denomination"] if denoms else "(sans nom)",
        "autres_noms": [d["Denomination"] for d in denoms[1:]],
        "status": silver.get("StatusLabel", ""),
        "forme_juridique": silver.get("JuridicalFormLabel", ""),
        "date_debut": silver.get("StartDate", ""),
        "adresse": None,
        "activites": [
            {"code": a.get("NaceCode"), "version": a.get("NaceVersion"),
             "label": a.get("NaceLabel", ""), "classification": a.get("Classification")}
            for a in acts
        ],
        "enriched": enriched,
    }
    if addrs:
        a = addrs[0]
        fiche["adresse"] = {
            "rue": a.get("StreetFR") or a.get("StreetNL", ""),
            "numero": a.get("HouseNumber", ""),
            "code_postal": a.get("Zipcode", ""),
            "commune": a.get("MunicipalityFR") or a.get("MunicipalityNL", ""),
        }
    gold = db.hotel_gold.find_one({"_id": num_raw}, {"_id": 0})
    fiche["financials"] = gold if gold else None
    return fiche

# gestion status
def _lire_statuts_hdfs(num_raw):
    try:
        with HDFS_API.read(f"/data/raw/notaire/{num_raw}/statutes.json", encoding="utf-8") as f:
            return json.loads(f.read())
    except Exception:
        return None

def _declencher_dag_notaire(num_raw):
    run_id = f"api__{num_raw}__{int(time.time())}"
    r = _rq.post(f"{AIRFLOW_API}/dags/notaire_ondemand/dagRuns",
                 json={"dag_run_id": run_id, "conf": {"numero": num_raw}},
                 auth=AIRFLOW_AUTH, timeout=15)
    if r.status_code not in (200, 201):
        return False
    for _ in range(45):
        time.sleep(2)
        s = _rq.get(f"{AIRFLOW_API}/dags/notaire_ondemand/dagRuns/{run_id}",
                    auth=AIRFLOW_AUTH, timeout=15)
        state = s.json().get("state")
        if state == "success":
            return True
        if state == "failed":
            return False
    return False

async def _stream_statuts(num_raw: str):
    # cache Mongo
    cache = db.notaire_cache.find_one({"_id": num_raw})
    if cache and "statutes" in cache:
        for st in cache["statutes"]:
            yield {"event": "document", "data": json.dumps(st, ensure_ascii=False)}
            await asyncio.sleep(0.05)
        yield {"event": "done", "data": json.dumps({"total": len(cache["statutes"]), "source": "mongo"})}
        return

    # déjà dans HDFS ?
    statuts = await asyncio.to_thread(_lire_statuts_hdfs, num_raw)

    # déclenche le DAG (scrape live)
    if statuts is None:
        yield {"event": "status", "data": json.dumps({"msg": "scraping en cours..."})}
        ok = await asyncio.to_thread(_declencher_dag_notaire, num_raw)
        if not ok:
            yield {"event": "error", "data": json.dumps({"msg": "scrape notaire échoué"})}
            yield {"event": "done", "data": json.dumps({"total": 0})}
            return
        statuts = await asyncio.to_thread(_lire_statuts_hdfs, num_raw)

    statuts = statuts or []
    for st in statuts:
        yield {"event": "document", "data": json.dumps(st, ensure_ascii=False)}
        await asyncio.sleep(0.1)
    db.notaire_cache.update_one({"_id": num_raw},
        {"$set": {"statutes": statuts, "count": len(statuts)}}, upsert=True)
    yield {"event": "done", "data": json.dumps({"total": len(statuts), "source": "scrape"})}

@app.get("/enterprise/{num}/statuts-stream")
async def statuts_stream(num: str):
    num_raw = re.sub(r"\D", "", num).zfill(10)
    return EventSourceResponse(_stream_statuts(num_raw))

# dirigeant
KBOPUB_URL = "https://kbopub.economie.fgov.be/kbopub/toonondernemingps.html"

def _scrape_dirigeants(num_raw):
    r = _rq.get(KBOPUB_URL, params={"lang": "fr", "ondernemingsnummer": num_raw},
                headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
    if r.status_code != 200:
        return []
    soup = BeautifulSoup(r.text, "html.parser")
    h2 = soup.find("h2", string=re.compile("Fonctions", re.I))
    if not h2:
        return []
    table = h2.find_parent("table")
    if not table:
        return []

    dirigeants = []
    for tr in table.find_all("tr"):
        cells = tr.find_all("td", class_=re.compile(r"^(QL|RL)$"))
        if len(cells) >= 2:
            fonction = cells[0].get_text(strip=True)
            nom = cells[1].get_text(" ", strip=True).replace("\xa0", " ")
            nom = re.sub(r"\s+", " ", nom).replace(" ,", ",").strip()
            depuis = cells[2].get_text(strip=True) if len(cells) >= 3 else ""
            if fonction and nom and ":" not in fonction and depuis:
                dirigeants.append({"fonction": fonction, "nom": nom, "depuis": depuis})
    return dirigeants

@app.get("/enterprise/{num}/dirigeants")
def dirigeants(num: str):
    num_raw = re.sub(r"\D", "", num).zfill(10)
    # cache Mongo
    cache = db.dirigeants_cache.find_one({"_id": num_raw})
    if cache and "dirigeants" in cache:
        return {"dirigeants": cache["dirigeants"], "source": "cache"}
    # sinon scrape kbopub
    num_kbo = num_raw.lstrip("0")
    dirs = _scrape_dirigeants(num_kbo)
    db.dirigeants_cache.update_one({"_id": num_raw},
        {"$set": {"dirigeants": dirs, "count": len(dirs)}}, upsert=True)
    return {"dirigeants": dirs, "source": "scrape"}

def _scrape_liens(num_kbo):
    r = _rq.get(KBOPUB_URL, params={"lang": "fr", "ondernemingsnummer": num_kbo},
                headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
    if r.status_code != 200:
        return []
    soup = BeautifulSoup(r.text, "html.parser")
    liens, seen = [], set()
    for a in soup.find_all("a", href=True):
        if "toonondernemingps.html" not in a["href"] or "ondernemingsnummer=" not in a["href"]:
            continue
        numero = a.get_text(" ", strip=True)
        if not re.match(r"\d{4}\.\d{3}\.\d{3}", numero):
            continue
        container, node = None, a
        for _ in range(5):
            node = node.parent
            if node is None:
                break
            if "depuis le" in node.get_text(" ", strip=True).lower():
                container = node
                break
        txt = container.get_text(" ", strip=True) if container else numero
        m_name = re.search(r"\((.+?)\)", txt)
        entite = m_name.group(1).strip() if m_name else ""
        after = txt.split(")", 1)[1] if ")" in txt else txt
        m = re.search(r"(.*?)\s*depuis le\s*(.+)$", after, re.I)
        nature = (m.group(1) if m else after).strip()
        date_lien = m.group(2).strip() if m else ""
        key = (numero, nature, date_lien)
        if key in seen:
            continue
        seen.add(key)
        liens.append({"entite": entite, "numero": numero,
                      "date_lien": date_lien, "nature": nature})
    return liens

@app.get("/enterprise/{num}/liens")
def liens(num: str):
    num_raw = re.sub(r"\D", "", num).zfill(10)
    cache = db.liens_cache.find_one({"_id": num_raw})
    if cache and "liens" in cache:
        return {"liens": cache["liens"], "source": "cache"}
    num_kbo = num_raw.lstrip("0")
    ls = _scrape_liens(num_kbo)
    db.liens_cache.update_one({"_id": num_raw},
        {"$set": {"liens": ls, "count": len(ls)}}, upsert=True)
    return {"liens": ls, "source": "scrape"}

def _scrape_entreprise_kbo(num_kbo):
    r = _rq.get(KBOPUB_URL, params={"lang": "fr", "ondernemingsnummer": num_kbo},
                headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
    if r.status_code != 200:
        return None
    soup = BeautifulSoup(r.text, "html.parser")
    infos = {}
    for tr in soup.find_all("tr"):
        cells = tr.find_all("td", class_=re.compile(r"^(QL|RL)$"))
        if len(cells) >= 2:
            label = cells[0].get_text(" ", strip=True).rstrip(":").strip()
            val_cell = cells[1]
            for span in val_cell.find_all("span", class_="upd"):
                span.extract()
            valeur = val_cell.get_text(" ", strip=True)
            valeur = re.sub(r"\s+", " ", valeur).strip()
            if label and valeur and "Pas de données" not in valeur:
                infos[label] = valeur

    if not infos.get("Dénomination"):
        return None
    num_fmt = _norm(num_kbo.zfill(10))
    nom = infos.get("Dénomination", "")
    statut = infos.get("Statut", "")
    forme = infos.get("Forme légale", "")
    date_debut = infos.get("Date de début", "")
    adresse_raw = infos.get("Adresse du siège", "")
    m = re.search(r"(.*?)\s+(\d{4})\s+(.+)$", adresse_raw)
    addresses = []
    if m:
        addresses = [{
            "TypeOfAddress": "REGO",
            "StreetFR": m.group(1).strip(),
            "HouseNumber": "",
            "Zipcode": m.group(2),
            "MunicipalityFR": m.group(3).strip(),
        }]

    doc = {
        "EnterpriseNumber": num_fmt,
        "denominations": [{"TypeOfDenomination": "001", "Denomination": nom}],
        "addresses": addresses,
        "activities": [],
        "StatusLabel": statut,
        "JuridicalFormLabel": forme,
        "StartDate": date_debut,
        "_source": "kbopub",
    }
    return doc