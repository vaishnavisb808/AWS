import os
import boto3

s3 = boto3.client('s3')
dynamodb = boto3.client('dynamodb')

def lambda_handler(event, context):
    uuid = event.get('uuid')
    file_type = event.get('file_type')
    indexes = event.get('indexes')
    
    final_file = f'/mnt/access/{uuid}/{uuid}.{file_type}'
    with open(final_file, 'w') as f:
        for index in indexes:
            path = f'/mnt/access/{uuid}/{uuid}_{index}.txt'
            if os.path.exists(path):
                file = open(path, 'r').read()
                f.write(file)
     
            else:
                return {'message': 'Some parts missing from efs'}
    
    bucket_name = 'tests3dynamo'
    s3.upload_file(final_file,bucket_name,f'{uuid}.{file_type}')
    
    table_name = 'file-process-tracker'
    key = {
    'uuid': {'S': uuid}
    }
    
    data_to_insert = "{'part':'completed'}"
    
    response = dynamodb.update_item(
        TableName=table_name,
        Key=key,
        UpdateExpression='SET is_processed = :data',
        ExpressionAttributeValues={
        ':data': {'BOOL': True}
    }
    )
    print(response)
    
    
    return {'message':'Successful!'}
            
            
        
