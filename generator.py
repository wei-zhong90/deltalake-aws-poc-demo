from faker import Faker
import json
from kafka import KafkaProducer
import time
import random
from orderproducer import OrderProvider
from dotenv import load_dotenv
import os

load_dotenv()


# Creating a Faker instance and seeding to have the same results every time we execute the script
fake = Faker()
Faker.seed(4321)


# function produce_msgs starts producing messages with Faker
def produce_msgs(hostname = 'hostname',
                 port = '1234',
                 topic_name = 'orders',
                 nr_messages = -1,
                 max_waiting_time_in_sec = 5):
    producer = KafkaProducer(
        bootstrap_servers=hostname + ':' + port,
        security_protocol='SSL',
        value_serializer=lambda v: json.dumps(v).encode('ascii'),
        key_serializer=lambda v: json.dumps(v).encode('ascii')
    )

    if nr_messages <= 0:
        nr_messages = float('inf')
    i = 0
    
    fake.add_provider(OrderProvider)
    
    while i < nr_messages:
        message, key = fake.produce_msg()

        print('Sending: {}'.format(message))
        # sending the message to Kafka
        producer.send(topic_name,
                      key=key,
                      value=message)
        # Sleeping time
        sleep_time = random.randint(0, int(max_waiting_time_in_sec * 10000))/10000
        print('Sleeping for...'+str(sleep_time)+'s')
        time.sleep(sleep_time)

        # Force flushing of all messages
        if (i % 100) == 0:
            producer.flush()
        i = i + 1
    producer.flush()


def main():
    p_hostname = os.environ['HOST']
    p_port = os.environ['PORT']
    p_topic_name = os.environ['TOPIC']
    nr_messages = os.environ['NR_MESSAGES']
    max_waiting_time = os.environ['MAX_WAIT_TIME']
    produce_msgs(hostname=p_hostname,
                 port=p_port,
                 topic_name=p_topic_name,
                 nr_messages=int(nr_messages),
                 max_waiting_time_in_sec=float(max_waiting_time)
                 )
    print("Finished producing " + nr_messages + " messages")

if __name__ == '__main__':
    main()