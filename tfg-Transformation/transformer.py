import pandas as pd
from io import StringIO
import base64
import json
import boto3
import os
import uuid
from datetime import datetime

def lambda_handler(event, context):
    print (event)
    jwt_string = event.get('records').get('transformation_topic-0')[0].get('value')
    print(jwt_string)
    decoded_jwt = base64.b64decode(jwt_string).decode('utf-8')
    print(decoded_jwt)
    data = json.loads(decoded_jwt)
    print(data)

    feed_type = data['feed_name']
    uuid = data['uuid']
    event = data['event']
    bucket_name = data['bucket_name']
    file_name = data['file_name']
    file_type = data['file_type']
    start = data['start']
    end = data['end']
    index = data['index']
    last = data['last']
    directory = '/mnt/access'
 

    # Create an instance of the transformer
    transformer = AMGtoSFCCLocationTransformer()
    
    # transformer = TransformerFactory.get_transformer(feed_type)
    transformer.transform(
        bucket_name=bucket_name,
        file_name=file_name,
        file_type=file_type,
        uuid=uuid,
        index=index,
        start=start,
        end=end,
        directory=directory
    )


class AMGtoSFCCLocationTransformer:
    def transform(self, bucket_name: str, file_name: str, file_type: str, uuid: str, index: int, start: int, end: int,
                      directory: str, **kwargs) -> None:
            
            # get data from s3 bucket
            data_string = self.get_data(bucket_name, file_name, start, end)
            # print(data_string)
            # treat the string as a file-like object
            data_file = StringIO(data_string)
            # print(data_file)
            # read the csv file
            data = pd.read_csv(data_file)
            # print(data)
            # add specified transformations
            transformed_data = self.add_transformations(data)
            
            # print (transformed_data)
            # Convert Data to XML
            xml_data = transformed_data.to_xml()
            # print(xml_data)


            print('xml_data')
            new_file_type = 'xml'
            self.update_file_type(uuid,new_file_type)
            # generate a batch file for given chunk
            self.generate_batch(directory, uuid, index, xml_data, new_file_type, start)
            print ("generated file")
  
           
    #required transformations for the csv file
    def add_transformations(self, data: pd.DataFrame) -> pd.DataFrame:
    # Create a dictionary to map old column names to new column names
            column_mapping = {
                'Code' : 'store-id',
                'Name' : 'name',
                'Address1' : 'address1',
                'Address2' : 'address2',
                'City' : 'city',
                'PostalCode' : 'postal-code',
                'Country' : 'country-code',
                'Phone' : 'phone',
                'Email' :'email',
                'Latitude' :'latitude',
                'Longitude' : 'longitude'
              }
        
            # Rename columns
            data.rename(columns=column_mapping, inplace=True)
        
            # Delete remaining columns
            columns_to_delete = data.columns.tolist()[15:]  # Exclude the renamed columns
            data.drop(columns=columns_to_delete, inplace=True)
            columns_to_drop = ['Type', 'CompanyName', 'State', 'Fax']
            data.drop(columns=columns_to_drop, inplace=True)
        
            # store-id column for transformation
            data['store-id'] = data['store-id'].apply(self.transform_store_id)
        
            return data  # Return the transformed data DataFrame

      
    #store-id transformation function
    def transform_store_id(self, store_id):
            store_id_str = str(store_id)
            if store_id_str.isdigit() and 1 <= int(store_id_str) <= 9:
                return "DC" + store_id_str.zfill(2)
            elif store_id_str.isdigit() and 10 <= int(store_id_str) <= 99:
                return "DC" + str(int(store_id_str)).lstrip('0')
            else:
                return int(store_id_str).lstrip('0')

    
    def update_db(self, uuid: str, index: int):
            dynamodb = boto3.client('dynamodb')
            table_name = 'file-process-tracker'
        
            key = {
                'uuid': {'S': uuid}
            }
        
            index = str(index)
        
            data_to_insert = f"{{'{index}':'completed'}}"
        
            response = dynamodb.update_item(
                TableName=table_name,
                Key=key,
                UpdateExpression='SET success_indexes = list_append(success_indexes, :data)',
                ExpressionAttributeValues={
                    ':data': {'L': [{'S': data_to_insert}]}
                }
            )
 
          
    def generate_batch(self, directory: str, uuid: str, index: int, data, file_type: str, start: int) -> None:
            if not os.path.exists(directory):
                os.mkdir(directory)
    
            if not os.path.exists(f'{directory}/{uuid}'):
                os.mkdir(f'{directory}/{uuid}')
    
            batch_name = f'{directory}/{uuid}/{uuid}_{index}.txt'
    
            if file_type == 'xml':
                if len(data) and start == 0:
                    data = '<?xml version="1.0" encoding="UTF-8"?>' + data
        
                with open(batch_name, 'w') as f:
                    f.write(data)
            else:
                data.to_csv(batch_name, index=False, header=(start == 0))
    
            self.update_db(uuid, index)
 
    
    def get_data(self, bucket_name: str, file_name: str, start: int, end: int) -> str:
            s3_client = boto3.client('s3')
            response = s3_client.get_object(Bucket=bucket_name, Key=file_name, Range=f'bytes={start}-{end}')
            data = response['Body'].read().decode('utf-8')
            return data
 

    def update_file_type(self, uuid: str, file_type: str):
            dynamodb = boto3.client('dynamodb')
            table_name = 'file-process-tracker'
            key = {
                'uuid': {'S': uuid}
            }
            expression_attribute_values = {
                ':file_type': {'S': file_type}
            }
            update_expression = 'SET file_type = :file_type'
        
            dynamodb.update_item(
                TableName=table_name,
                Key=key,
                UpdateExpression=update_expression,
                ExpressionAttributeValues=expression_attribute_values
            )

        
          
