import sys

from kafka import KafkaConsumer

# Kafka configuration
bootstrap_servers = 'localhost:9092'
# Topic can be passed as an argument, e.g. `python consume_messages.py result1`
# (use result1 or result2 to verify that Flink produces the wanted results)
topic = sys.argv[1] if len(sys.argv) > 1 else 'sensors'

# Create a Kafka consumer
consumer = KafkaConsumer(topic,
                         bootstrap_servers=bootstrap_servers,
                         group_id='my_consumer_group',
                         auto_offset_reset='earliest',  # Start reading from the beginning if no offset is stored
                         enable_auto_commit=False)  # Disable auto-commit to manually control offsets

try:
	for message in consumer:
		print(f"Received message: {message.value.decode('utf-8')}")
except KeyboardInterrupt:
	pass
finally:
	# Close the consumer
	consumer.close()
