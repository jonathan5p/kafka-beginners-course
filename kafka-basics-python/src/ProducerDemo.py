from confluent_kafka import Producer

conn_props = {
    "bootstrap.servers": "magnetic-finch-6283-us1-kafka.upstash.io:9092",
    "security.protocol": "SASL_SSL",
    "sasl.username": "username",
    "sasl.password": "password",
    "sasl.mechanism": "SCRAM-SHA-256",
}

producer = Producer(conn_props)

try:
    producer.produce("demo_java", b"Hello from python")
    print("Message produced without Avro schema!")
except Exception as e:
    print(f"Error producing message: {e}")
finally:
    producer.flush()
