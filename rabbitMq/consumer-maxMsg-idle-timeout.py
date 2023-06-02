import pika
import boto3
import sys
import json
import time

# Create a Secrets Manager client
secrets_manager_client = boto3.client('secretsmanager')

def get_rabbitmq_credentials(secret_arn):
    # Retrieve the secret value
    response = secrets_manager_client.get_secret_value(SecretId=secret_arn)

    # Parse the secret value
    if 'SecretString' in response:
        secret_data = json.loads(response['SecretString'])
    else:
        # Handle binary secret values if necessary
        secret_data = json.loads(response['SecretBinary'])

    # Access the required secret values
    rabbitmq_user = secret_data['username']
    rabbitmq_pass = secret_data['password']
    rabbitmq_host = secret_data['host']
    rabbitmq_port = secret_data['port']

    return rabbitmq_user, rabbitmq_pass, rabbitmq_host, rabbitmq_port

def lambda_handler(event, context):
    queue_name = "aws-queue-1"
    max_messages = 50000  # Maximum number of messages to consume
    message_count = 0  # Counter for processed messages
    idle_timeout = 20  # Idle timeout in seconds

    secret_arn = "arn:aws:secretsmanager:eu-west-2:264567323125:secret:dev/ens/rabbitMQ-TiW38Z"

    # Retrieve RabbitMQ credentials from Secrets Manager
    rabbitmq_user, rabbitmq_pass, rabbitmq_host, rabbitmq_port = get_rabbitmq_credentials(secret_arn)

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=rabbitmq_host, port=rabbitmq_port, credentials=pika.PlainCredentials(rabbitmq_user, rabbitmq_pass))
    )

    channel = connection.channel()
    channel.queue_declare(queue=queue_name)
    # Track the timestamp of the last received message
    last_message_time = time.time()  

    def callback(ch, method, properties, body):
        nonlocal last_message_time, message_count
        # Process the received message
        message = body.decode('utf-8').split(',')
        print("Received message:", body)
        message_count += 1

        # Update the last message timestamp
        last_message_time = time.time()

        if message_count >= max_messages:
            # Stop consuming messages
            print("Maximum message count reached")
            channel.stop_consuming()
            return

    # Start consuming messages from the queue
    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)

    try:
        # Consume messages until the maximum message count is reached or idle timeout occurs
        while message_count < max_messages:
            channel.connection.process_data_events()

            # Check for idle timeout
            if time.time() - last_message_time > idle_timeout:
                print("Idle timeout reached")
                break

    except KeyboardInterrupt:
        # Gracefully stop consuming if interrupted
        print("Consumer stopped due to interruption")
        pass

    # Close the connection
    connection.close()
