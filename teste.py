from io import StringIO
import boto3
import pandas as pd
from dotenv import load_dotenv, dotenv_values
import datetime

AWS_SECRET_ACCESS_KEY = dotenv_values('.env')['AWS_SECRET_ACCESS_KEY']
AWS_ACCESS_KEY_ID     = dotenv_values('.env')['AWS_ACCESS_KEY_ID']  
REGION_NAME           = dotenv_values('.env')['REGION_NAME'] 
BUCKET                = 'databricks-spark-streaming' 

df = pd.DataFrame([{'A': 1, 'B': 2}])

destination = "output_" + str(datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S')) + '.json'


s3 = boto3.client("s3",\
                  region_name=REGION_NAME,\
                  aws_access_key_id=AWS_ACCESS_KEY_ID,\
                  aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
json_buf = StringIO()
df.to_json(json_buf)
json_buf.seek(0)
s3.put_object(Bucket=BUCKET, Body=json_buf.getvalue(), Key='landing/'+destination)
