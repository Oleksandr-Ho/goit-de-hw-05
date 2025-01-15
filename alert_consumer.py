from kafka import KafkaConsumer
from configs import kafka_config
import json

# Назви топіків для сповіщень
temperature_alerts_topic = 'oholodetskyi_temperature_alerts'
humidity_alerts_topic = 'oholodetskyi_humidity_alerts'

# Створюємо Consumer для підписки на обидва топіки
consumer = KafkaConsumer(
    temperature_alerts_topic,
    humidity_alerts_topic,
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda v: v.decode('utf-8') if v else None,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='alert_consumer_group_oholodetskyi'
)

# Підписка на топіки
print("Subscribed to temperature and humidity alert topics")

# Зчитування сповіщень
try:
    for message in consumer:
        print(f"Received alert from topic: {message.topic}, Key: {message.key}, Value: {message.value}")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    consumer.close()
