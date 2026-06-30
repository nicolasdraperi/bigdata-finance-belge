import json, re, time
from bs4 import BeautifulSoup
import requests
from hdfs import InsecureClient
import os
from pathlib import Path

HDFS_URL    = "http://namenode:9870"
HDFS_USER   = "root"
RAW_HDFS    = "/data/raw"
_hdfs       = InsecureClient(HDFS_URL, user=HDFS_USER)


HEADERS  = {"User-Agent": "Mozilla/5.0 (compatible; KBO-notebook/1.0)"}
BASE_URL = "https://kbopub.economie.fgov.be/kbopub/toonondernemingps.html"
KBO      = "https://kbopub.economie.fgov.be/kbopub"
EJ_BASE  = "https://www.ejustice.just.fgov.be/cgi_tsv/list.pl"
BROKER   = "https://consult.cbso.nbb.be/api/external/broker/public/deposits"
CBSO_API = "https://consult.cbso.nbb.be/api/rs-consult/published-deposits"
CBSO_HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}

ENTREPRISES = ["0878065378", "0836157420", "0203430576"]

import csv

def get_entreprises_from_csv(limit=None):
    path = Path("/opt/airflow/data/enterprise.csv")
    if not path.exists():
        raise FileNotFoundError(f"Fichier introuvable : {path}")
    numeros = []
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            numero = (row.get("EnterpriseNumber") or row.get("enterprise_number")
                      or row.get("enterpriseNumber") or row.get("Number") or row.get("number"))
            if not numero:
                continue
            numero = re.sub(r"\D", "", numero).zfill(10)
            if numero:
                numeros.append(numero)
            if limit and len(numeros) >= limit:
                break
    return numeros

LOCAL_CSV_DIR = "/opt/airflow/data"
CSV_HDFS_DIR = "/data/raw/kbo_registry"

def ingest_local_csv_to_hdfs():
    csv_dir = Path(LOCAL_CSV_DIR)

    if not csv_dir.exists():
        raise FileNotFoundError(f"Dossier CSV introuvable : {LOCAL_CSV_DIR}")

    files = list(csv_dir.glob("*.csv"))

    if not files:
        raise FileNotFoundError(f"Aucun CSV trouvé dans : {LOCAL_CSV_DIR}")

    _hdfs.makedirs(CSV_HDFS_DIR)

    uploaded = []

    for file in files:
        hdfs_path = f"{CSV_HDFS_DIR}/{file.name}"

        with open(file, "rb") as f:
            _hdfs.write(hdfs_path, data=f.read(), overwrite=True)

        uploaded.append(hdfs_path)

    return uploaded
def save_raw(source, num_url, filename, content):
    numero = re.sub(r"\D", "", num_url).zfill(10)
    path = f"{RAW_HDFS}/{source}/{numero}/{filename}"
    data = content if isinstance(content, (bytes, bytearray)) else content.encode("utf-8")
    parent = path.rsplit("/", 1)[0]
    _hdfs.makedirs(parent)
    _hdfs.write(path, data=data, overwrite=True)
    return path

def _get(url, **kw):
    kw.setdefault("headers", HEADERS); kw.setdefault("timeout", 30)
    r = requests.get(url, **kw); r.encoding = "utf-8"
    return r

def _num9(num_url):
    return re.sub(r"\D", "", num_url).lstrip("0")

def _btw(num_url):
    return re.sub(r"\D", "", num_url).zfill(10)

def statuts_kbo(num_url, limit=5, max_pages=50, cookie=None):
    numero = re.sub(r"\D", "", num_url).zfill(10)
    url = f"https://statuts.notaire.be/stapor_v1/api/enterprises/{numero}/statutes"
    headers = {**HEADERS, "Accept": "application/json, text/plain, */*",
               "Referer": f"https://statuts.notaire.be/stapor_v1/enterprise/{numero}/statutes",
               "X-Requested-With": "XMLHttpRequest"}
    if cookie:
        headers["Cookie"] = cookie
    items, offset, total = [], 0, None
    for _ in range(max_pages):
        try:
            r = requests.get(url, params={"deedDate": "", "offset": offset, "limit": limit},
                             headers=headers, timeout=20)
        except Exception:
            break
        if r.status_code != 200 or "json" not in r.headers.get("content-type", "").lower():
            break
        data = r.json()
        items.extend(data.get("statutes", []))
        total = data.get("totalItems", total)
        offset += limit
        if total is not None and offset >= total:
            break
    return items

def cbso_deposits(numero, size=100):
    numero = numero.replace(".", "").zfill(10)
    items, page = [], 0
    while True:
        params = {"page": page, "size": size, "enterpriseNumber": numero,
                  "sort": ["periodEndDate,desc", "depositDate,desc"]}
        r = requests.get(CBSO_API, headers=CBSO_HEADERS, params=params, timeout=30)
        if r.status_code != 200:
            break
        data = r.json()
        batch = data.get("content", [])
        items.extend(batch)
        if data.get("last", True) or not batch:
            break
        page += 1; time.sleep(0.3)
    return items

def _is_fr(dep):
    return (dep.get("language") or "").upper() == "FR"

def _is_consolidated(dep):
    mid  = (dep.get("modelId") or "").lower()
    name = (dep.get("modelName") or "").lower()
    return mid.startswith(("m120", "m122", "mc")) or "consolid" in name or "geconsolideerde" in name

def comptes_retenus(numero, an_min=2021, an_max=2025):
    numero = numero.replace(".", "").zfill(10)
    par_an = {}
    for d in cbso_deposits(numero):
        if _is_consolidated(d):
            continue
        y = d.get("periodEndDateYear")
        if not (isinstance(y, int) and an_min <= y <= an_max):
            continue
        cur = par_an.get(y)
        if cur is None:
            par_an[y] = d
        else:
            cand_better = (_is_fr(d) and not _is_fr(cur))
            cur_is_p, cand_is_p = cur.get("modelId","").endswith("-p"), d.get("modelId","").endswith("-p")
            if cand_better or (cur_is_p and not cand_is_p):
                par_an[y] = d
    return dict(sorted(par_an.items(), reverse=True))

def ingest_kbopub_fiche(num_url):
    r = _get(BASE_URL, params={"lang": "fr", "ondernemingsnummer": num_url})
    save_raw("kbopub", num_url, "entreprise.html", r.text)

def ingest_kbopub_etablissements(num_url, max_etabs=None):
    numero = _num9(num_url)
    page, vus = 1, []
    while True:
        r = _get(f"{KBO}/vestiginglijst.html",
                 params={"lang": "fr", "ondernemingsnummer": numero, "page": page})
        if r.status_code == 404:
            break
        save_raw("kbopub", num_url, f"etablissements_p{page}.html", r.text)
        soup  = BeautifulSoup(r.text, "html.parser")
        liens = soup.find_all("a", href=re.compile(r"toonvestigingps\.html\?vestigingsnummer="))
        if not liens:
            break
        for a in liens:
            vus.append(re.sub(r"\D", "", a.get_text()))
        m = re.search(r"(\d+)\s+(?:vestigingseenheden|unité|unités|établissement)", r.text, re.I)
        total = int(m.group(1)) if m else None
        if total is not None and len(vus) >= total:
            break
        page += 1; time.sleep(0.25)
    if max_etabs:
        vus = vus[:max_etabs]
    for vest in vus:
        rv = _get(f"{KBO}/toonvestigingps.html", params={"lang": "fr", "vestigingsnummer": vest})
        if rv.status_code == 200:
            save_raw("kbopub", num_url, f"vestiging_{vest}.html", rv.text)
        time.sleep(0.25)

def ingest_ejustice(num_url):
    page = 1
    while page <= 100:
        r = _get(EJ_BASE, params={"language": "fr", "btw": _btw(num_url), "page": page})
        if r.status_code != 200:
            break
        soup = BeautifulSoup(r.text, "html.parser")
        if not soup.select("div.list-item--content"):
            break
        save_raw("ejustice", num_url, f"page_{page}.html", r.text)
        page += 1; time.sleep(0.3)


SEED_BCE    = "0836157420"
COOKIE_HDFS = "/data/raw/_cookies/notaire.txt"

def _cookie_valide(cookie_str):
    if not cookie_str:
        return False
    try:
        r = requests.get(
            f"https://statuts.notaire.be/stapor_v1/api/enterprises/{SEED_BCE}/statutes",
            params={"offset": 0, "limit": 1},
            headers={**HEADERS, "Accept": "application/json", "Cookie": cookie_str},
            timeout=10)
        return "application/json" in r.headers.get("content-type", "")
    except Exception:
        return False

def _renouveler_cookie():
    from pyvirtualdisplay import Display
    from playwright.sync_api import sync_playwright
    seed = (f"https://statuts.notaire.be/stapor_v1/enterprise/{SEED_BCE}/statutes"
            f"?enterpriseNumber={SEED_BCE}&statuteStart=0&statuteCount=5")
    with Display(visible=False, size=(1400, 1000)):
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=False,
                args=["--no-sandbox", "--disable-dev-shm-usage"])
            ctx  = browser.new_context(locale="fr-BE")
            page = ctx.new_page()
            page.add_init_script(
                "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")
            page.goto("https://statuts.notaire.be/", wait_until="load", timeout=20000)
            page.wait_for_timeout(2000)
            page.goto(seed, wait_until="load", timeout=30000)
            for _ in range(40):
                names = {c["name"] for c in ctx.cookies()}
                if "Lyp1CWKh" in names and ("OCImoOot" in names or "OClmoOot" in names):
                    break
                page.wait_for_timeout(500)
            cookies = ctx.cookies()
            browser.close()
    return "; ".join(f"{c['name']}={c['value']}" for c in cookies)

def get_cookie_notaire(force=False):
    if not force:
        try:
            with _hdfs.read(COOKIE_HDFS, encoding="utf-8") as f:
                cached = f.read().strip()
            if _cookie_valide(cached):
                return cached
        except Exception:
            pass
    cookie = _renouveler_cookie()
    _hdfs.makedirs(COOKIE_HDFS.rsplit("/", 1)[0])
    _hdfs.write(COOKIE_HDFS, data=cookie, overwrite=True)
    return cookie

def ingest_notaire(num_url, cookie=None):
    cookie = cookie or get_cookie_notaire()
    items  = statuts_kbo(num_url, cookie=cookie)
    save_raw("notaire", num_url, "statutes.json",
             json.dumps(items, ensure_ascii=False, indent=2))

def ingest_cbso(num_url):
    numero = re.sub(r"\D", "", num_url).zfill(10)
    save_raw("cbso", num_url, "deposits.json",
             json.dumps(cbso_deposits(numero), ensure_ascii=False, indent=2))
    h_bin = {"User-Agent": "Mozilla/5.0", "Accept": "*/*",
             "Referer": "https://consult.cbso.nbb.be/"}
    for annee, dep in comptes_retenus(numero).items():
        did = dep["id"]
        rp = _get(f"{BROKER}/pdf/{did}", headers=h_bin)
        if rp.status_code == 200 and rp.content:
            save_raw("cbso", num_url, f"pdf/{annee}.pdf", rp.content)
        rx = _get(f"{BROKER}/xbrl/{did}", headers=h_bin)
        if rx.status_code == 200 and rx.content:
            save_raw("cbso", num_url, f"xbrl/{annee}.xbrl", rx.content)
        if annee >= 2021:
            rc = _get(f"{BROKER}/consult/csv/{did}", headers=h_bin)
            if rc.status_code == 200 and rc.content:
                save_raw("cbso", num_url, f"csv/{annee}.csv", rc.content)
        time.sleep(0.4)