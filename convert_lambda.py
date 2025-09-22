import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io 

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    
    #get the bucket and key
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    #download the csv files from the bucket
    obj = s3.get_object(Bucket=bucket, Key=key)
    csv_content = obj['Body'].read().decode('utf-8')

    #read the csv into a pandas data frame
    df = pd.read_csv(io.StringIO(csv_content))

    #convert into parquet
    table = pa.Table.from_pandas(df)

    #prepare for uploading
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)

    #replace the key with parquet
    dest_key = key.replace('.csv', '.parquet')

    #upload parquet to s3 bucket
    s3.put_object(Bucket='finstream-parquet', Key=dest_key, Body=buffer.getvalue())

    return {'status': 'converted', 'file': key}
