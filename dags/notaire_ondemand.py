from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.models.param import Param
import sys, json
sys.path.append("/opt/airflow/include")
import bce_ingestion as ing

@dag(
    dag_id="notaire_ondemand",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    params={"numero": Param("", type="string")},
    tags=["notaire", "ondemand"],
)
def notaire_ondemand():

    @task(execution_timeout=timedelta(minutes=5))
    def scrape(**context):
        numero = context["params"]["numero"]
        cookie = ing.get_cookie_notaire(allow_renew=True)
        items = ing.statuts_kbo(numero, cookie=cookie)
        ing.save_raw("notaire", numero, "statutes.json",
                     json.dumps(items, ensure_ascii=False, indent=2))
        return {"numero": numero, "count": len(items)}

    scrape()

notaire_ondemand()