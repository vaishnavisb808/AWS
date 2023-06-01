
import json
import pika
import boto3

s3_client = boto3.client("s3")

def lambda_handler(event, context):
    
    #S3 
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    s3_file_name = event['Records'][0]['s3']['object']['key']

    resp = s3_client.get_object(Bucket = bucket_name, Key = s3_file_name)
    data = resp['Body'].read().decode('utf-8').splitlines()
    print (data)
    
    # Retrieve RabbitMQ credentials
    rabbitmq_host = "35.178.200.222"
    rabbitmq_port = 5672  # Default RabbitMQ port
    rabbitmq_user = "rabbitmquser"
    rabbitmq_pass = "Welcome@123"
    
    # Create a connection to RabbitMQ
    parameters = pika.ConnectionParameters(host=rabbitmq_host, port=rabbitmq_port,
                                           credentials=pika.PlainCredentials(rabbitmq_user, rabbitmq_pass))
    
    connection = pika.BlockingConnection(parameters)
   
    # Create a channel
    channel = connection.channel()
   
    # Declare a queue
    queue_name = "aws-queue-1"
    channel.queue_declare(queue=queue_name)
    
    # Publish a message
    # message = "Hello, RabbitMQ!"
    for message in data:
        channel.basic_publish(  exchange="", 
                            routing_key=queue_name, 
                            body=message)
    
    # Close the connection
    connection.close()

