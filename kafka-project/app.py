from kafka import KafkaProducer, KafkaConsumer
import json
import sys
import time

sys.stdout = open(sys.stdout.fileno(), 'w', buffering=1)

def produce():
    print("Starting producer...", flush=True)
    producer = KafkaProducer(
        bootstrap_servers=['kafka-0.kafka-svc.kafka.svc.cluster.local:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("Producer connected!", flush=True)
    for i in range(10):
        msg = {'order_id': i, 'product': f'item-{i}', 'amount': i * 100}
        producer.send('orders', msg)
        print(f"Sent: {msg}", flush=True)
        time.sleep(1)
    producer.close()
    print("Producer finished", flush=True)

def consume():
    print("Starting consumer...", flush=True)
    print("Connecting to Kafka...", flush=True)
    consumer = KafkaConsumer(
        'orders',
        bootstrap_servers=['kafka-0.kafka-svc.kafka.svc.cluster.local:9092'],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        group_id='consumer-group',
        consumer_timeout_ms=60000
    )
    print("Consumer connected! Waiting for messages...", flush=True)
    try:
        for msg in consumer:
            print(f"Received: {msg.value}", flush=True)
    except Exception as e:
        print(f"Consumer stopped: {e}", flush=True)
    print("Consumer exiting", flush=True)

if __name__ == '__main__':
    print(f"Starting app with args: {sys.argv}", flush=True)
    if len(sys.argv) > 1 and sys.argv[1] == 'produce':
        produce()
    else:
        consume()
