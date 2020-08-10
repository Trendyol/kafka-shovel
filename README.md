# Description
**Kafka shovel moves error topic to retry topic. It indicates topics from ERROR prefix then replace it to RETRY.Error topics must have ERROR prefix and retry topics must have RETRY prefix.It runs every 15 minutes.**

# Required environment variables
	KAFKAVERSION = 2.1.2
        RETRYCOUNT = 5
	ERRORPREFIX = ERROR
	RETRYPREFIX = RETRY
	TOPICS=topicname:true/false (for infinite retry)
	RUNNINGTIME = 5 (its closed after)
	BROKERS =  kafka cluster address with port (ex: 1.0.0.1:9092,1.0.0.2:9092)
	KAFKAVERSION = kafka version (ex: 2.0.0)

# Usages
    docker run aonuryilmaz/kafka-shovel
    
