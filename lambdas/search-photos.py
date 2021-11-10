import json
import math
import dateutil.parser
import datetime
import time
import os
import logging
import boto3
import requests
import urllib.parse
from aws_requests_auth.aws_auth import AWSRequestsAuth
from elasticsearch import Elasticsearch, RequestsHttpConnection
import random

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

headers = { "Content-Type": "application/json" }
region = 'us-east-1'
lex = boto3.client('lex-runtime', region_name=region)

def lambda_handler(event, context):
    print(event)
    print("THIS IS THE DAMN EVENT")
    q1 = event["queryStringParameters"]['q']
    labels = get_labels(q1)
    
    print(labels)
    
    if len(labels) != 0:
        img_paths = get_photo_path(labels)
    
    print(img_paths)
    
    if not img_paths:
        return{
            'statusCode':200,
            "headers": {"Access-Control-Allow-Origin":"*"},
            'body': json.dumps('No Results found')
        }
    else:    
        return{
            'statusCode': 200,
            'headers': {"Access-Control-Allow-Origin":"*"},
            'body': {
                'imagePaths':img_paths,
                'userQuery':q1,
                'labels': labels,
            },
            'isBase64Encoded': False
        }
    
def get_labels(query):
    sample_string = 'pqrstuvwxyabdsfbc'
    userid = ''.join((random.choice(sample_string)) for x in range(8))
    
    response = lex.post_text(
        botName='picpiper',                 
        botAlias='first_piper',
        userId=userid,           
        inputText=query
    )
    # print("lex-response", response)
    
    labels = []
    if 'slots' not in response:
        print("No photo collection for query {}".format(query))
    else:
        print ("slot: ",response['slots'])
        slot_val = response['slots']
        for key,value in slot_val.items():
            if value!=None:
                labels.append(value)
    return labels

    
def get_photo_path(keys):
    host="search-piper-es-2vi7inr55exmidpivownvkwgqu.us-east-1.es.amazonaws.com"
    awsauth = AWSRequestsAuth(aws_access_key='AKIAZ43I5RB2K367DHFN',
                      aws_secret_access_key='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',
                      aws_host=host,
                      aws_region='us-east-1',
                      aws_service='es')
    
    es = Elasticsearch(
        hosts=[{'host': host, 'port':443}],
        use_ssl=True,
        http_auth=awsauth,
        verify_certs=True,
        connection_class=RequestsHttpConnection)
        
    
    resp = []
    for key in keys:
        if (key is not None) and key != '':
            searchData = es.search({"query": {"match": {"labels": key}}})
            resp.append(searchData)
    # print(resp)

    output = []
    for r in resp:
        if 'hits' in r:
             for val in r['hits']['hits']:
                key = val['_source']['objectKey']
                if key not in output:
                    output.append('s3://picpiper/'+key)
    # print (output)
    return output  
