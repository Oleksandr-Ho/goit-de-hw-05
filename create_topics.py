from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config

# Створення клієнта Kafka
admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)

# Список топіків
topics_to_create = [
    NewTopic(name='building_sensors_oholodetskyi', num_partitions=3, replication_factor=1),
    NewTopic(name='temperature_alerts_oholodetskyi', num_partitions=1, replication_factor=1),
    NewTopic(name='humidity_alerts_oholodetskyi', num_partitions=1, replication_factor=1)
]

# Створення топіків
try:
    admin_client.create_topics(new_topics=topics_to_create, validate_only=False)
    print("Topics created successfully.")
except Exception as e:
    print(f"An error occurred: {e}")

# Перевірка створених топіків
print("Existing topics:")
print([topic for topic in admin_client.list_topics() if "oholodetskyi" in topic])

# Закриття з'єднання
admin_client.close()
