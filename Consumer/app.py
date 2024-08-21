import os
import pika
import pymongo
import json
from statistics import mean


def fail_on_error(err, msg):
    if err:
        print(f"{msg}: {err}")
        raise Exception(msg)


def process_messages(channel, method, properties, body, collection):
    messages = []
    messages.append(json.loads(body))

    if len(messages) == 1000:
        prices = [msg['price'] for msg in messages]
        average_price = mean(prices)
        collection.insert_one({"average_price": average_price})
        print(f"Stored average price: {average_price}")
        messages.clear()


def main():
    rabbitmq_url = os.getenv('RABBITMQ_URL', 'amqp://guest:guest@localhost:5672/')
    mongo_url = os.getenv('MONGO_URL', 'mongodb://localhost:27017/')
    queue_name = os.getenv('QUEUE_NAME', 'stock_queue')

   
    connection = pika.BlockingConnection(pika.URLParameters(rabbitmq_url))
    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=True)

   
    client = pymongo.MongoClient(mongo_url)
    db = client['financial_data']
    collection = db['aggregated_data']

    
    for method_frame, properties, body in channel.consume(queue_name):
        process_messages(channel, method_frame, properties, body, collection)
        channel.basic_ack(method_frame.delivery_tag)

    connection.close()

if __name__ == "__main__":
    main()
