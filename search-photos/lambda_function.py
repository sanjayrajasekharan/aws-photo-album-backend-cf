from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import boto3
import requests
import json
import os



def lambda_handler(event, context):
    
    print("Received event: " + str(event))

    search_query = event["queryStringParameters"]["q"]
    print(f"Search query: {search_query}")

    #CALL LEX HERE
    lexv2_client = boto3.client('lexv2-runtime')
    
    bot_id = 'C2UHC0NLBK'
    bot_alias_id = 'TSTALIASID' 
    locale_id = 'en_US'  
    session_id = 'YOUR_SESSION_ID'
    
    lex_response = lexv2_client.recognize_text(
        botId=bot_id,
        botAliasId=bot_alias_id,
        localeId=locale_id,
        sessionId=session_id,
        text=search_query
    )
    
    print("Response from Lex" + str(lex_response))
    slots = lex_response["interpretations"][0]["intent"]["slots"]
    
    keyword1 = slots["keyword1"]["value"]["interpretedValue"] if slots and slots["keyword1"] else None
    keyword2 = slots["keyword2"]["value"]["interpretedValue"] if slots and slots["keyword2"] else None
    
    disambiguated_words = []
    
    if keyword1: 
        disambiguated_words.append(keyword1)
        disambiguated_words.append(keyword1.lower())
        if keyword1[-1] == 's':
            disambiguated_words.append(keyword1[:-1])
    if keyword2: 
        disambiguated_words.append(keyword2)
        disambiguated_words.append(keyword2.lower())
        if keyword2[-1] == 's':
            disambiguated_words.append(keyword2[:-1])

    # Define the search query
    search_body = {
        "query": {
            "terms": {
                "labels": disambiguated_words
            }
        }
    }

    # OpenSearch Service domain
    opensearch_domain = os.environ['OPENSEARCH_DOMAIN']
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

    # Construct the URL for the _search endpoint
    url = f"{opensearch_domain}/photos/_search"

    # Send the GET request
    response = requests.get(url, auth=aws_auth, json=search_body)

    # Parse the response JSON
    response_json = response.json()
    print("Response json" + str(response_json))
    
    # Print the search results
    for hit in response_json['hits']['hits']:
        print(hit['_source'])
        
        url = boto3.client('s3').generate_presigned_url(
        ClientMethod='get_object', 
        Params={'Bucket': 'b2-image-store', 'Key': hit["_source"]["objectKey"]},
        ExpiresIn=3600)
        
        hit["signed_url"] = url

    return {
    'statusCode': 200,
    'headers': {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',  # Allow any origin (not recommended for production)
        'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',  # Allowed headers
        'Access-Control-Allow-Methods': 'GET,OPTIONS'  # Allowed methods
    },
    'body': json.dumps(response_json)
}