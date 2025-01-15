from kafka import KafkaConsumer, KafkaProducer
from configs import kafka_config
import json

# Назва топіку
building_sensors_topic = 'oholodetskyi_building_sensors'
temperature_alerts_topic = 'oholodetskyi_temperature_alerts'
humidity_alerts_topic = 'oholodetskyi_humidity_alerts'

# Створюємо Consumer для зчитування даних із топіку building_sensors
consumer = KafkaConsumer(
    building_sensors_topic,
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my_consumer_group_oholodetskyi'
)

# Створюємо Producer для відправки сповіщень у відповідні топіки
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
)

# Обробка отриманих даних
print(f"Subscribed to topic '{building_sensors_topic}'")
for message in consumer:
    sensor_data = message.value
    sensor_id = sensor_data['sensor_id']
    temperature = sensor_data['temperature']
    humidity = sensor_data['humidity']

    print(f"Received data from Sensor ID: {sensor_id}, Temperature: {temperature}°C, Humidity: {humidity}%")

    # Перевірка на високі значення температури
    if temperature > 40:
        alert = {
            "sensor_id": sensor_id,
            "temperature": temperature,
            "timestamp": sensor_data['timestamp'],
            "alert": "High Temperature Alert!"
        }
        producer.send(temperature_alerts_topic, value=alert)
        print(f"Temperature alert sent: {alert}")

    # Перевірка на вихід рівня вологості за межі
    if humidity > 80 or humidity < 20:
        alert = {
            "sensor_id": sensor_id,
            "humidity": humidity,
            "timestamp": sensor_data['timestamp'],
            "alert": "Humidity Alert!"
        }
        producer.send(humidity_alerts_topic, value=alert)
        print(f"Humidity alert sent: {alert}")

# Закриваємо producer
producer.close()
