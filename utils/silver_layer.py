from datetime import timedelta
import os
from dotenv import load_dotenv
import boto3
load_dotenv()

class silverLayer:
    def __init__(self):
        pass

    def trigger_spark_job(self,job_name,job_parameter):
       aws_access_key_id=os.environ.get("aws_access_key_id")
       aws_secret_access_key=os.environ.get("aws_secret_access_key")

       #create a glue client
       glue_client=boto3.client(
           'glue',
        region_name="ap-southeast-2",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
       )

       #start glue client
       response=glue_client.start_job_run(
           JobName=job_name,
           Arguments={
               '--load_date':job_parameter
           }
          

       )
       return response['JobRunId']

    
    def trigger_crawler(self,crawler_name):
        aws_access_key_id=os.environ.get("aws_access_key_id")
        aws_secret_access_key=os.environ.get("aws_secret_access_key")

        #create a glue client
        glue_client=boto3.client(
            'glue',
            region_name="ap-southeast-2",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )
        #start the glue crawler
        response=glue_client.start_crawler(
            Name=crawler_name
        )

        return response
    

if __name__=="__main__":
    obj=silverLayer()
    crawler_response=obj.trigger_crawler("crawler_silver")
    print(crawler_response)
    



