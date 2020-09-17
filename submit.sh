#!/bin/bash

PARALLELISM=1
JDBC_URL="jdbc:postgresql://timescaledb:5432/twitch?user=postgres&password=password" 
KAFKA_BOOTSTRAP_SERVER="kafka:9092"
KAFKA_TOPIC="TwitchMessages"
AGGREGATION_INTERVAL_MS=900000 # 15 min, event-time
TRIGGER_INTERVAL_MS=5000 # 5 sec, processing-time
MAX_OUT_OF_ORDERNESS_MS=10000 # 10 sec, event-time


docker cp ./build/libs/twitch-chat-analyzer-1.1-SNAPSHOT.jar tca_flink-jobmanager:/analyser.jar

# Same directory mounted into jm and tm
docker exec -u root tca_flink-jobmanager mkdir -p /data/checkpoints
docker exec -u root tca_flink-jobmanager chown flink:flink /data/checkpoints

docker exec -ti tca_flink-jobmanager flink run -p $PARALLELISM -c de.prkz.twitch.emoteanalyser.EmoteAnalyser /analyser.jar \
	$JDBC_URL \
	$KAFKA_BOOTSTRAP_SERVER \
	$KAFKA_TOPIC \
	$AGGREGATION_INTERVAL_MS \
	$TRIGGER_INTERVAL_MS \
	$MAX_OUT_OF_ORDERNESS_MS
