from datetime import datetime
from airflow.decorators import dag, task

@dag(dag_id="test_hdfs", schedule=None, start_date=datetime(2025,1,1), catchup=False)
def test_hdfs():
    @task
    def ping_hdfs():
        from hdfs import InsecureClient
        client = InsecureClient("http://namenode:9870", user="root")
        client.makedirs("/data/nbb")
        client.write("/data/nbb/_test.txt", data="hello hdfs", overwrite=True)
        print("Contenu /data :", client.list("/data"))
    ping_hdfs()

test_hdfs()