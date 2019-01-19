#!/bin/bash

PARALLELISM=1
JDBC_URL="jdbc:postgresql://db:5432/twitch?user=postgres&password=password" 
KAFKA_BOOTSTRAP_SERVER="kafka:9092"
AGGREGATION_INTERVAL_MS=900000 # 15 min, event-time
TRIGGER_INTERVAL_MS=5000 # 5 sec, processing-time
MAX_OUT_OF_ORDERNESS_MS=60000 # 1 min, event-time
DB_BATCH_SIZE=1 # rows, batch-size for channel_stats is 1/5th of that or at least 1


docker cp ./target/emote-analyzer-1.0-SNAPSHOT.jar tca_flink-jobmanager:/analyser.jar

# Same directory mounted into jm and tm
docker exec -u root tca_flink-jobmanager mkdir -p /data/checkpoints
docker exec -u root tca_flink-jobmanager chown flink:flink /data/checkpoints

docker exec -ti tca_flink-jobmanager flink run -p $PARALLELISM -c de.prkz.twitch.emoteanalyser.EmoteAnalyser /analyser.jar \
	$JDBC_URL \
	$KAFKA_BOOTSTRAP_SERVER \
	$AGGREGATION_INTERVAL_MS \
	$TRIGGER_INTERVAL_MS \
	$MAX_OUT_OF_ORDERNESS_MS \
	$DB_BATCH_SIZE
