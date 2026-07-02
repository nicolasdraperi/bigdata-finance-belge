from datetime import datetime, timedelta
from airflow.decorators import dag, task
import sys
sys.path.append("/opt/airflow/include")
import bce_gold as gold

default_args = {"retries": 1, "retry_delay": timedelta(minutes=5)}

@dag(
    dag_id="gold_annuel",
    schedule="0 2 1 4 *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["gold", "annuel", "incremental"],
    is_paused_upon_creation=True,
)
def gold_annuel():

    @task(execution_timeout=timedelta(hours=6))
    def detecter():
        return gold.detecter_nouveaux_depots()

    @task(execution_timeout=timedelta(hours=6))
    def recalculer(nums):
        return gold.gold_incremental(nums)

    nums = detecter()
    recalculer(nums)

gold_annuel()