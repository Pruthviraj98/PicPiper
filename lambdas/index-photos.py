import json
import boto3
from aws_requests_auth.aws_auth import AWSRequestsAuth
import time
from elasticsearch import Elasticsearch, RequestsHttpConnection
import requests
import io
import base64
from base64 import decodestring

def lambda_handler(event, context):
    S3_details=event['Records'][0]['s3']
    bucket=event['Records'][0]['s3']['bucket']['name']
    photo=event['Records'][0]['s3']['object']['key']
    timeStamp=time.time()
    s3_client = boto3.client('s3')
    s3_clientobj = s3_client.get_object(Bucket=bucket, Key=photo)
    body=s3_clientobj['Body'].read().decode('utf-8')
    image = base64.b64decode(body)        
    
    
    response=s3_client.delete_object(Bucket=bucket,Key=photo)
    response=s3_client.put_object(Bucket=bucket, Body=image, Key=photo,ContentType='image/jpg')
    
    
    client=boto3.client('rekognition')
    response = client.detect_labels(
    Image={'Bytes':image},
    MaxLabels=10,
    MinConfidence=80
    )
    
    
    labels=response['Labels']
    custom_labels=[]
    for label in labels:
        custom_labels.append(label['Name'])
   
    format={
        'objectKey':photo,
        'bucket':bucket,
        'createdTimeStamp':timeStamp,
        'labels':custom_labels
    }
    
    host="search-piper-es-2vi7inr55exmidpivownvkwgqu.us-east-1.es.amazonaws.com"
    
    es_payload=json.dumps(format).encode("utf-8")
    awsauth = AWSRequestsAuth(aws_access_key='',
                      aws_secret_access_key='',
                      aws_host=host,
                      aws_region='us-east-1',
                      aws_service='es')
    
    esClient = Elasticsearch(
        hosts=[{'host': host, 'port':443}],
        use_ssl=True,
        http_auth=awsauth,
        verify_certs=True,
        connection_class=RequestsHttpConnection)
        
    temp=esClient.index(index='photos', doc_type='photo', body=format)
    print(temp)
