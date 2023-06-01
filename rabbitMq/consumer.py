import pika

def on_message_delivered(ch, method, properties, body):
    print(f"received new message: {body}")

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare(queue='hello')

channel.basic_consume(queue='hello', auto_ack=True, on_message_callback= on_message_delivered)
print("Start consuming")

channel.start_consuming()