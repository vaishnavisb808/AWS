import boto3
import json

dynamodb = boto3.client('dynamodb')

def lambda_handler(event, context):
    table_name = 'file-process-tracker'
    
    query_params = {
        "TableName": table_name,
        "FilterExpression": "is_processed = :value",
        "ExpressionAttributeValues": {":value": {"BOOL": False}},
    }
    
    response = dynamodb.scan(**query_params)
    items = response["Items"]
    
    for item in items:
        indexes = item.get('indexes').get('NS')
        success_indexes = item.get('success_indexes').get('L')
        
        
        if len(indexes)==len(success_indexes):
            indexes = list(map(int, indexes))
            indexes.sort()
            data = {
                'uuid' : item.get('uuid').get('S'),
                'indexes' : indexes ,
                'file_type' : item.get('file_type').get('S')
            }
            
            print(data)
            
            function_name = 'amg-sfcc-location-aggregator'
            
            payload = json.dumps(data).encode('utf-8')
            client = boto3.client('lambda')
            response = client.invoke(
                 FunctionName=function_name,
                 InvocationType='Event',
                 Payload=payload
             )
