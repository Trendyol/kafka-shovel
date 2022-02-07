# Description
**Kafka shovel moves error topic to retry topic. It indicates topics from ERRORSUFFIX suffix then replace it to RETRYSUFFIX.Error topics must have ERRORSUFFIX suffix and retry topics must have RETRYSUFFIX suffix. Default , It runs every 5 minutes.**

# Required environment variables
	KAFKAVERSION = 2.1.2
    RETRYCOUNT = 5
	ERRORSUFFIX = ERROR
	RETRYPSUFFIX = RETRY
	TOPICS=topicname:true/false (for infinite retry)
	RUNNINGTIME = 5 (its closed after)
	BROKERS =  kafka cluster address with port (ex: 1.0.0.1:9092,1.0.0.2:9092)
	KAFKAVERSION = kafka version (ex: 2.0.0)
	DURATION = 5 (minutes)

# Usages
    docker run aonuryilmaz/kafka-shovel
    
