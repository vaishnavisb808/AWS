import json

import boto3
import json
from kafka import KafkaProducer
import uuid
from datetime import datetime

s3 = boto3.client('s3')
dynamodb = boto3.client('dynamodb')

producer = KafkaProducer(bootstrap_servers='b-2.ensmsk.70lsne.c2.kafka.eu-west-2.amazonaws.com:9092,b-1.ensmsk.70lsne.c2.kafka.eu-west-2.amazonaws.com:9092,b-3.ensmsk.70lsne.c2.kafka.eu-west-2.amazonaws.com:9092')
table_name = 'file-process-tracker'


configMaster = {
    "DEMANDWAREmasterCatalog" : {
        "delimiter" : "</product>",
        "splitThreshold" : 50, #in MB, Optional Parameter default 256
        "noOfChunks" : 9 # Optional Parameter default 9
    },
    "albertron" :{
        "delimiter" : "\n", #Optional Parameter for csv
        "splitThreshold" : 50, #in MB, Optional query_params = {
        "TableName": table_name,
        "FilterExpression": "isProcessed = :value",
        "ExpressionAttributeValues": {":value": {"BOOL": False}},
    }
    }

def lambda_handler(event, context):
    bucket_name, file_name = get_bucket_and_file(event)
    print(f"bucket_name [{bucket_name}] file_name [{file_name}]")
    file_size = get_file_size(bucket_name, file_name)
    print(f"file_size [{file_size}]")
    
    file_type, feed_name = get_file_details(file_name)
    print(f"file_type [{file_type}]")

    config = get_config(feed_name)
    
    if file_size < config.get('splitThreshold', 256) * 1024 * 1024:  # Check if file size is less than config splitThreshold in MB
        send_single_event(bucket_name, file_name, file_size, file_type, feed_name)
    else:
        try:
            split_file(bucket_name, file_name, file_size, file_type, config, feed_name)
        except Exception as e:
            print(f"Error [{e}]")

def get_config(feed_name):
    config = configMaster.get(feed_name, {}) #Later on we can use some sort of config management
    return config

def get_bucket_and_file(event):
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_name = event['Records'][0]['s3']['object']['key']
    return bucket_name, file_name

def get_file_size(bucket_name, file_name):
    response = s3.head_object(Bucket=bucket_name, Key=file_name)
    return response['ContentLength']
    
def get_file_details(file_key):
    file_type = file_key.split('.')[-1]
    feed_name = (file_key.split('/')[-1]).split('-')[0]
    # feed_name = (file_key.split('/')[-1]).split('_')[0]
    return file_type.lower(), feed_name
    
def send_single_event(bucket_name, file_name, file_size, file_type, feed_name):
    u = str(uuid.uuid4())
    send_event(u, bucket_name, file_name, 0, file_size - 1, 0, True, file_type, feed_name)
    send_aggregate_event(u, [0], file_type, file_name, bucket_name)

def split_file(bucket_name, file_name, file_size, file_type, config, feed_name):
    split_points = calculate_split_points(file_size, config.get('noOfChunks',9))
    print(f"split_points [{split_points}]")
    adjusted_splits = adjust_split_points(bucket_name, file_name, split_points, file_type, config)
    print(f"adjusted_splits [{adjusted_splits}]")
    u = str(uuid.uuid4())
    send_events_to_msk(u, bucket_name, file_name, adjusted_splits, file_type, feed_name)

def calculate_split_points(file_size, noOfChunks):
    return [i * file_size // noOfChunks for i in range(1, noOfChunks+1)]

def adjust_split_points(bucket_name, file_name, split_points, file_type, config):
    adjusted_splits = []
    for split_point in split_points:
        adjusted_splits.append(adjust_split_point(bucket_name, file_name, split_point, file_type, config))
    return adjusted_splits

def adjust_split_point(bucket_name, file_name, split_point, file_type, config):
    split_buffer = 256
    buffer_limit = 1024 * 1024 * 15 #Taking 15MB as maximum buffer limit 
    counter = 0
    
    while True:
        response = s3.get_object(Bucket=bucket_name, Key=file_name, Range=f'bytes={split_point-split_buffer}-{split_point+split_buffer}')
        fragment = response['Body'].read()
        if file_type == 'xml':
            delimiter = config.get('delimiter')
            if delimiter == None:
                raise Exception('Please Configure the delimiter.')
            closing_tag_index = fragment.rfind(delimiter.encode())
            if closing_tag_index == -1:
                if split_buffer > buffer_limit:
                    raise Exception('Invalid file.')
                split_buffer *= 2
                continue
            start_index = split_point - split_buffer
            byte_index = start_index + closing_tag_index + len(delimiter) - 1
            return byte_index
        
        elif file_type == 'csv' or file_type == 'go':
            delimiter = config.get('delimiter','\n')
            newline_position = fragment[split_buffer:].find(delimiter.encode())
            if newline_position == -1:
                if counter > 4:
                    return split_point
                split_buffer *= 2
                counter += 1
                continue
            return split_point + newline_position

def send_events_to_msk(uuid, bucket_name, file_name, adjusted_splits, file_type, feed_name):
    n = len(adjusted_splits)
    for i in range(n):
        start = adjusted_splits[i-1]+1 if i > 0 else 0
        end = adjusted_splits[i]
        send_event(uuid, bucket_name, file_name, start, end, i, i == (n - 1), file_type, feed_name)

    indexes = list(range(n))
    send_aggregate_event(uuid, indexes, file_type, file_name, bucket_name)
    producer.flush()

def send_event(uuid, bucket_name, file_name, start, end, index, last, file_type, feed_name):
    payload = {
        "uuid": uuid,
        "event": "transform",
        "bucket_name": bucket_name,
        "file_name": file_name,
        "start": start,
        "end": end,
        "index": index,
        "last": last,
        "file_type": file_type,
        "feed_name" : feed_name
    }

    print(payload)
    topic = 'transformation_topic'
    print(topic)
    # topic = 'splitter-events'
    payload_bytes = json.dumps(payload).encode('utf-8')
    send_kafka_event(payload_bytes, topic)
    print("Message sent!")


def send_aggregate_event(uuid, indexes, file_type, file_name, bucket_name):
    payload = {
        'uuid': uuid,
        'event': 'aggregate',
        'indexes': indexes,
        'file_type': file_type
    }
    topic = 'new-topic'
    send_kafka_event(json.dumps(payload).encode('utf-8'), topic)
    item = {
        'uuid' : {'S':uuid},
        'filename' : {'S':file_name},
        'bucket' : {'S':bucket_name},
        'indexes' : {'NS':list(map(str,indexes))} ,
        'success_indexes' : {'L':[]},
        'failed_indexes' : {'L':[]},
        'created_at': {'S' : datetime.now().astimezone().isoformat()},
        'updated_at' : {'S' : datetime.now().astimezone().isoformat()},
        'file_type' : {'S' : file_type},
        'is_processed' : {'BOOL' : False}
    }
    
    response = dynamodb.put_item(
        TableName =table_name,
        Item=item
    )
    print(response)

def send_kafka_event(payload_bytes, topic):
    future = producer.send(topic, payload_bytes)

    try:
        record_metadata = future.get(timeout=10)
    except KafkaError:
        # Decide what to do if produce request failed...
        log.exception()
        pass

    print(topic , payload_bytes)
    
