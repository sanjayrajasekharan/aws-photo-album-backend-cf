import json
import boto3
from boto3 import client, Session
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from datetime import datetime

from botocore.credentials import AssumeRoleCredentialFetcher, DeferredRefreshableCredentials
from botocore.session import Session

def lambda_handler(event, context):
    # TODO implement
    print("Received event: " + json.dumps(event, indent=2))

    rekognition = client('rekognition')
    s3 = client('s3')

    bucket = event['Records'][0]['s3']['bucket']['name']
    if not bucket:
        bucket = "b2-image-store-cf" 
    key = event['Records'][0]['s3']['object']['key']
    print("Bucket: " + bucket)

    #key = key.replace("+", " ")

    # Get object metadata
    responseS3 = s3.head_object(Bucket=bucket, Key=key)
    metadata = responseS3["Metadata"]
    print(metadata)

    if "customlabels" in metadata:
        custom_labels_array = json.loads(metadata["customlabels"])
        custom_labels_array = [label.lower() for label in custom_labels_array]
        print(custom_labels_array)
    else:
        custom_labels_array = []

    response = rekognition.detect_labels(
        Image={
            'S3Object': {
                'Bucket': bucket,
                'Name': key
            }
        },
        MaxLabels=10,
        MinConfidence=75
    )

    print("Detected labels for the image:")
    for label in response['Labels']:
        lower_case_label = label['Name'].lower()
        custom_labels_array.append(lower_case_label)
        print("{} - Confidence: {:.2f}".format(label['Name'], label['Confidence']))

    print(custom_labels_array)

    store_opensearch(key, bucket, custom_labels_array)

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }


def store_opensearch(key, bucket, labels):
    # OpenSearch Service domain
    opensearch_domain = 'https://search-photos-cf-2-efqs2sx2tdlysrwaygoztmmn4m.us-east-1.es.amazonaws.com'

    region = 'us-east-1'
    
    session = boto3.Session()
    credentials = session.get_credentials()
    aws_auth = AWS4Auth(
        credentials.access_key,
        credentials.secret_key,
        region,
        'es',
        session_token=credentials.token
    )

    openSearch = OpenSearch(
        hosts=[{'host': opensearch_domain.replace('https://', ''), 'port': 443}],  # Remove 'https://' from the host
        http_auth=aws_auth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    )

    # Document to be indexed
    document = {
        "objectKey": key,
        "bucket": bucket,
        "createdTimestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "labels": labels
    }

    print(document)

    # Indexing the document
    response = openSearch.index(index='photos-cf', body=document)