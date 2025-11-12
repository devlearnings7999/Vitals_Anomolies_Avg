import json
import os
import time
from collections import defaultdict

from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError


load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER")
INPUT_TOPIC = os.getenv("INPUT_TOPIC")
OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC")
SECURITY_PROTOCOL = os.getenv("SECURITY_PROTOCOL")
SASL_MECHANISM = os.getenv("SASL_MECHANISM")
SASL_USERNAME = os.getenv("SASL_USERNAME")
SASL_PASSWORD = os.getenv("SASL_PASSWORD")


def consume_and_average():
    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        security_protocol=SECURITY_PROTOCOL,
        sasl_mechanism=SASL_MECHANISM,
        sasl_plain_username=SASL_USERNAME,
        sasl_plain_password=SASL_PASSWORD,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        security_protocol=SECURITY_PROTOCOL,
        sasl_mechanism=SASL_MECHANISM,
        sasl_plain_username=SASL_USERNAME,
        sasl_plain_password=SASL_PASSWORD,
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

    vitals_data = defaultdict(lambda: {"sum": 0, "count": 0})
    last_published = time.time()
    interval = 10  # seconds

    try:
        while True:
            for message in consumer:
                data = message.value
                
                vitals = ['body_temp', 'heart_rate', 'systolic', 'diastolic', 'breaths', 'oxygen', 'glucose']
                for vital in vitals:
                    if vital in data:
                        vitals_data[vital]["sum"] += data[vital]
                        vitals_data[vital]["count"] += 1
                    else:
                        print(f"Warning: {vital} not found in message: {data}")

                current_time = time.time()
                if current_time - last_published >= interval:
                    if any(vitals_data[vital]["count"] > 0 for vital in vitals):
                        averages = {}
                        for vital in vitals:
                            if vitals_data[vital]["count"] > 0:
                                averages[vital] = vitals_data[vital]["sum"] / vitals_data[vital]["count"]
                            else:
                                averages[vital] = 0  # Avoid division by zero
                            vitals_data[vital]["sum"] = 0
                            vitals_data[vital]["count"] = 0

                        print(f"Averaged vitals: {averages}")

                        try:
                            producer.send(OUTPUT_TOPIC, value=averages)
                            producer.flush()
                        except KafkaError as e:
                            print(f"Error sending message: {e}")

                        last_published = current_time
                    else:
                        print("No messages received in the last 10 seconds.")
                        last_published = current_time

    except KeyboardInterrupt:
        print("Consumer stopped by user.")
    finally:
        consumer.close()
        producer.close()


if __name__ == "__main__":
    consume_and_average()