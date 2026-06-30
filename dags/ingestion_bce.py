from datetime import datetime, timedelta
from airflow.decorators import dag, task
import sys

sys.path.append("/opt/airflow/include")
import bce_ingestion as ing

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=2)
}

@dag(
    dag_id="ingestion_bce",
    schedule="@monthly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["bce", "ingestion", "hdfs"],
)
def ingestion_bce():

    @task
    def lister_entreprises() -> list[str]:
        return ing.get_entreprises_from_csv(limit=10)

    @task
    def prep_cookie():
        ing.get_cookie_notaire(force=True)
        return "ok"

    @task
    def t_csv_local():
        return ing.ingest_local_csv_to_hdfs()

    @task(retries=2)
    def t_kbopub(num: str):
        ing.ingest_kbopub_fiche(num)
        ing.ingest_kbopub_etablissements(num, max_etabs=20)
        return num

    @task
    def t_ejustice(num: str):
        ing.ingest_ejustice(num)
        return num

    @task(retries=5, retry_delay=timedelta(minutes=3))
    def t_notaire(num: str):
        ing.ingest_notaire(num)
        return num

    @task
    def t_cbso(num: str):
        ing.ingest_cbso(num)
        return num

    nums = lister_entreprises()

    cookie_ready = prep_cookie()

    csv_done = t_csv_local()

    kbopub_tasks = t_kbopub.expand(num=nums)
    ejustice_tasks = t_ejustice.expand(num=nums)
    cbso_tasks = t_cbso.expand(num=nums)
    notaire_tasks = t_notaire.expand(num=nums)

    csv_done >> [kbopub_tasks, ejustice_tasks, cbso_tasks]

    cookie_ready >> notaire_tasks


ingestion_bce()