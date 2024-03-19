# Description
**Kafka shovel moves error topic to retry topic. It indicates topics from ERRORSUFFIX then replace it to RETRYSUFFIX.Error topics must have ERRORSUFFIX suffix and retry topics must have RETRYSUFFIX suffix. Default , It runs every 5 minutes.**

# Required environment variables
	KAFKAVERSION = kafka version (ex: 2.0.0)
 	GROUPNAME = shovel
 	RETRYCOUNT = 5
	ERRORSUFFIX = ERROR
	RETRYSUFFIX = RETRY
	TOPICS=topicname:true/false (for infinite retry)
	BROKERS =  kafka cluster address with port (ex: 1.0.0.1:9092,1.0.0.2:9092)
	DURATION = 5 (minutes)

# Usages
    docker build -t kafka-shovel .
    docker run kafka-shovel
    
