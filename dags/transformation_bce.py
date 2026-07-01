from datetime import datetime, timedelta
from airflow.decorators import dag, task
import sys
sys.path.append("/opt/airflow/include")
import bce_silver as silver

default_args = {"retries": 1, "retry_delay": timedelta(minutes=2)}

@dag(
    dag_id="transformation_bce",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["bce", "silver", "transformation"],
)
def transformation_bce():

    @task(execution_timeout=timedelta(hours=2))
    def t_fusion():
        return silver.build_enterprise_finale()

    @task(execution_timeout=timedelta(hours=2))
    def t_dates():
        return silver.silver_normalize_dates()

    @task(execution_timeout=timedelta(hours=2))
    def t_dedup():
        return silver.silver_dedup_activities()

    @task(execution_timeout=timedelta(hours=2))
    def t_address():
        return silver.silver_address_rego()

    @task(execution_timeout=timedelta(hours=2))
    def t_denom():
        return silver.silver_denomination_principale()

    @task(execution_timeout=timedelta(hours=2))
    def t_labels():
        return silver.silver_decode_labels()
    @task(execution_timeout=timedelta(hours=1))
    def t_target_hotels():
        return silver.target_hotellerie()
    @task(execution_timeout=timedelta(hours=2))
    def t_scrape_hotels():
        return silver.scrape_hotels_nbb(batch=20)

    fusion = t_fusion()
    dates = t_dates()
    dedup = t_dedup()
    address = t_address()
    denom = t_denom()
    labels = t_labels()
    hotels = t_target_hotels()

    fusion >> dates >> dedup >> address >> denom >> labels >> hotels >> t_scrape_hotels()


transformation_bce()