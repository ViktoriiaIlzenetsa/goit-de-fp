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


# Визначення нового топіку
my_name = "vika"
topic_names= [f'{my_name}_athlete_event_results_input', f'{my_name}_athlete_event_results_output']

input_topic = NewTopic(name=topic_names[0], num_partitions=2, replication_factor=1)
output_topic = NewTopic(name=topic_names[1], num_partitions=1, replication_factor=1)


# Створення нового топіку
try:
    admin_client.create_topics(new_topics=[input_topic, output_topic], validate_only=False)
    print(f"Topics {topic_names} created successfully.")
except Exception as e:
    print(f"An error occurred: {e}")

# Перевіряємо список існуючих топіків 
[print(topic) for topic in admin_client.list_topics() if my_name in topic]

# Закриття зв'язку з клієнтом
admin_client.close()

