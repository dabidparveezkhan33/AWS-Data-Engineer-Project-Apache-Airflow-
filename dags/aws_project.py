import os
import sys 
from datetime import timedelta,datetime
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from utils.bronze_layer import bronzeLayer
from utils.silver_layer import silverLayer

from airflow.sdk import dag,task


@dag(
    schedule="@daily",
    is_paused_upon_creation=False,
)
def aws_project_dag():
    @task.python(retries=3, retry_delay=timedelta(seconds=10))
    def extract_load_s3():

        obj=bronzeLayer()

        folder_name = datetime.now().strftime("%Y-%m-%d")
        #fetching data from the urls and loading it to s3 bucket

        urls=["https://raw.githubusercontent.com/dabidparveezkhan33/AWS-Data-Engineer-Project-Apache-Airflow-/refs/heads/main/bookings.csv",
              "https://raw.githubusercontent.com/dabidparveezkhan33/AWS-Data-Engineer-Project-Apache-Airflow-/refs/heads/main/passengers.csv",
              "https://raw.githubusercontent.com/dabidparveezkhan33/AWS-Data-Engineer-Project-Apache-Airflow-/refs/heads/main/airports.csv"
              ]
        for url in urls:
            fetched_data=obj.ingest_data_api(url)
            obj.put_data_s3_hook("dk33-s3-bucket",f"bronze/{folder_name}/{url.split('/')[-1]}",fetched_data)
        return folder_name
    

    @task.python(retries=3, retry_delay=timedelta(seconds=10))
    def transform_load_s3(ti):
        #fetching the last load date from the extract_load_s3 task

        last_load_date= ti.xcom_pull(task_ids="extract_load_s3", key="return_value")
        obj=silverLayer()

        #triggering the spark job in the aws glue to transform the data and load it to s3 bucket
        job_run_id=obj.trigger_spark_job("silver_layer",last_load_date) 
        print(f"Glue job Triggered with job run id: {job_run_id}")

    @task.python(retries=3, retry_delay=timedelta(seconds=10))
    def transform_load_s3_parquet(ti):
            #fetching the last load date from the extract_load_s3 task
    
        last_load_date= ti.xcom_pull(task_ids="extract_load_s3", key="return_value")
        obj=silverLayer()
    
         #triggering the spark job in the aws glue to transform the data and load it to s3 bucket
        job_run_id=obj.trigger_spark_job("silver_layer_athena",last_load_date) 
        print(f"Glue job Triggered with job run id: {job_run_id}")


    #task to trigger the glue crawler to update the data catalog

    @task.python(retries=3, retry_delay=timedelta(seconds=10))
    def trigger_crawler():
        obj=silverLayer()
        crawler_response=obj.trigger_crawler("crawler_silver")
        print(crawler_response)


    #creating the dag dependencies
    extract_load_s3=extract_load_s3() #type: ignore
    transform_load_s3=transform_load_s3() #type: ignore
    transform_load_s3_parquet=transform_load_s3_parquet() #type: ignore
    trigger_crawler=trigger_crawler() #type: ignore

    extract_load_s3 >> [transform_load_s3,transform_load_s3_parquet] >> trigger_crawler  #type: ignore
   

aws_dag=aws_project_dag()

     
        









