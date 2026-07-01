from datetime import datetime, timedelta
from airflow.decorators import dag, task
import sys
sys.path.append("/opt/airflow/include")
import bce_ingestion as ing

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
        return ing.build_enterprise_finale()

    t_fusion()

transformation_bce()