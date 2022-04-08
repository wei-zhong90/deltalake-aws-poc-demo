from faker import Faker
import json
from kafka import KafkaProducer
import time
import random
from orderproducer import OrderProvider
from dotenv import load_dotenv
import os
import multiprocessing

load_dotenv()


# Creating a Faker instance and seeding to have the same results every time we execute the script
fake = Faker()
Faker.seed(4321)

MEMBERSHIPS = ["Copper", "Silver", "Gold", "Diamond"]

# function produce_msgs starts producing messages with Faker


def produce_msgs(bootstrap_servers="BOOTSTRAP_SERVERS",
                 topic_name='orders',
                 nr_messages=-1,
                 max_waiting_time_in_sec=5,
                 second_topic=False):
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
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

        if second_topic:
            order_owner = message['order_owner']
            membersihp = random.choice(MEMBERSHIPS)
            timestamp = message['timestamp']
            message = {"order_owner": order_owner,
                       "membership": membersihp, "timestamp": timestamp}

        print('Sending: {}'.format(message))
        # sending the message to Kafka
        producer.send(topic_name,
                      key=key,
                      value=message)
        # Sleeping time
        sleep_time = random.randint(
            0, int(max_waiting_time_in_sec * 10000))/10000
        print('Sleeping for...'+str(sleep_time)+'s')
        time.sleep(sleep_time)

        # Force flushing of all messages
        if (i % 100) == 0:
            producer.flush()
        i = i + 1
    producer.flush()


def main():
    p_bootstrap_servers = os.environ['BOOTSTRAP_SERVERS']
    p_topic_name = os.environ['TOPIC']
    p_second_topic_name = os.environ['SECOND_TOPIC']
    nr_messages = os.environ['NR_MESSAGES']
    max_waiting_time = os.environ['MAX_WAIT_TIME']

    p1 = multiprocessing.Process(target=produce_msgs, kwargs={
        "bootstrap_servers": p_bootstrap_servers,
        "topic_name": p_topic_name,
        "nr_messages": int(nr_messages),
        "max_waiting_time_in_sec": float(max_waiting_time)})

    p2 = multiprocessing.Process(target=produce_msgs, kwargs={
        "bootstrap_servers": p_bootstrap_servers,
        "topic_name": p_second_topic_name,
        "nr_messages": int(nr_messages),
        "max_waiting_time_in_sec": float(max_waiting_time),
        "second_topic": True})

    # produce_msgs(bootstrap_servers=p_bootstrap_servers,
    #              topic_name=p_topic_name,
    #              nr_messages=int(nr_messages),
    #              max_waiting_time_in_sec=float(max_waiting_time)
    #              )
    p1.start()
    p2.start()
    p1.join()
    p2.join()
    print("Finished producing " + nr_messages + " messages")


if __name__ == '__main__':
    main()
