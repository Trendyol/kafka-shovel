# Description
**It moves error topic to retry topic. It indicates topics from ERROR prefix then replace it to RETRY.Error topics must have ERROR prefix and retry topics must have RETRY prefix.It runs every 15 minutes.**

# Required environment variables
	BROKERS :  kafka cluster address with port (ex: 1.0.0.1:9092,1.0.0.2:9092)
	KAFKA_VERSION : kafka version (ex: 2.0.0)

# Usages
    docker run aonuryilmaz/kafka-shovel
    