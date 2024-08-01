from confluent_kafka import Consumer

conn_props = {
    "bootstrap.servers": "magnetic-finch-6283-us1-kafka.upstash.io:9092",
    "security.protocol": "SASL_SSL",
    "sasl.username": "bWFnbmV0aWMtZmluY2gtNjI4MySfSq0Zf46fRByTfelaaxz7I9v8ZL8atkb41sI",
    "sasl.password": "MmE3YTBmMjQtMGVlOS00ZjljLWE4NzgtNGI4MmMzNmRjMTFm",
    "sasl.mechanism": "SCRAM-SHA-256",
    "group.id": "my-python-application",
    "auto.offset.reset": "earliest",
}

c = Consumer(conn_props)

c.subscribe(["demo_java"])

while True:
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    print("Received message: {}".format(msg.value().decode("utf-8")))

c.close()
