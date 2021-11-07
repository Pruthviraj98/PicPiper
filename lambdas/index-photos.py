import json
import boto3
from aws_requests_auth.aws_auth import AWSRequestsAuth
import time
from elasticsearch import Elasticsearch, RequestsHttpConnection
import requests

def lambda_handler(event, context):
    # S3_details=event['Records'][0]['s3']
    
    bucket=event['Records'][0]['s3']['bucket']['name']
    photo=event['Records'][0]['s3']['object']['key']
    timeStamp=time.time()
    
    client=boto3.client('rekognition')
    
    response = client.detect_labels(Image={'S3Object': {'Bucket': bucket, 'Name': photo}}, MaxLabels=10, MinConfidence=80)   
        
    print(response)
    
    
    labels=response['Labels']
    custom_labels=[]
    for label in labels:
        if(label['Confidence']>40):
            custom_labels.append(label['Name'])
   
    photo="abc1.jpg"
    bucket="pqrs1"
    custom_labels=['abc', 'pqr']
    
    print(custom_labels)
    
    format={
        'objectKey':photo,
        'bucket':bucket,
        'createdTimeStamp':timeStamp,
        'labels':custom_labels
    }
    
    host="search-picpiper-es-nswdvekospzycu2hyhwmad5aki.us-east-1.es.amazonaws.com"
    
    es_payload=json.dumps(format).encode("utf-8")
    awsauth = AWSRequestsAuth(aws_access_key='AKIAZ43I5RB2K367DHFN',
                      aws_secret_access_key='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',
                      aws_host=host,
                      aws_region='us-east-1',
                      aws_service='es')
    
    esClient = Elasticsearch(
        hosts=[{'host': host, 'port':443}],
        use_ssl=True,
        http_auth=awsauth,
        verify_certs=True,
        connection_class=RequestsHttpConnection)
        
    esClient.index(index='photos', doc_type='photo', body=format)