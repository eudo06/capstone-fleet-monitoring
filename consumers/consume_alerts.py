from confluent_kafka import Consumer, KafkaException

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "alerts-notifier",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
    "partition.assignment.strategy": "cooperative-sticky"
})


def on_assign(consumer, partitions):
    print(f"Partitions attribuées : {partitions}")


def on_revoke(consumer, partitions):
    print(f"Partitions retirées : {partitions}")


consumer.subscribe(
    ["alerts"],
    on_assign=on_assign,
    on_revoke=on_revoke
)

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print(f"Erreur Kafka : {msg.error()}")
            continue

        print(f"Alerte reçue : {msg.value().decode('utf-8')}")

        consumer.commit(message=msg, asynchronous=False)

        print("Offset confirmé")

except KeyboardInterrupt:
    print("Arrêt du consommateur")

finally:
    consumer.close()


