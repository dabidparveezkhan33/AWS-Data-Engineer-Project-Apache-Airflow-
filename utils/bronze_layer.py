class bronzeLayer:

    def __init__(self):
        pass

    def ingest_data_api(self,url):
        import pandas as pd
        import requests
        from io import StringIO

        response=requests.get(url)

        if response.status_code==200:
            data=response.text
            df=pd.read_csv(StringIO(data))

            #convert the dataframe to a text csv file 
            csv_buffer=StringIO()
            df.to_csv(csv_buffer,index=False)

            #print the csv content
            return csv_buffer.getvalue()


    def put_data_s3_hook(self,bucket_name,object_key,data):
        import boto3
        import os
        from dotenv import load_dotenv 
        load_dotenv()  

        aws_access_key_id=os.environ.get("aws_access_key_id")
        aws_secret_access_key=os.environ.get("aws_secret_access_key")

        s3_client = boto3.client(
            's3',
            region_name="ap-southeast-2",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )

        #upload the data to s3 bucket
        s3_client.put_object(
            Bucket=bucket_name,
            Key=object_key,
            Body=data.encode("utf-8")

        )

        return f"Data successfully uploaded to S3 bucket '{bucket_name}' with object key '{object_key}'"

    


#obj=bronzeLayer()
#data=obj.ingest_data_api("https://raw.githubusercontent.com/dabidparveezkhan33/AWS-Data-Engineer-Project-Apache-Airflow-/refs/heads/main/bookings.csv")
#obj.put_data_s3_hook("dk33-s3-bucket","bronze/bookings.csv",data)
            





            


        