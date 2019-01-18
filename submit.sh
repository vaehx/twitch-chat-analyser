#!/bin/bash

docker cp ./target/emote-analyzer-1.0-SNAPSHOT.jar flink-jobmanager:/analyser.jar

docker exec -u root tca_flink-jobmanager mkdir -p /data/checkpoints
docker exec -u root tca_flink-jobmanager chown flink:flink /data/checkpoints

docker exec -u root tca_flink-taskmanager mkdir -p /data/checkpoints
docker exec -u root tca_flink-taskmanager chown flink:flink /data/checkpoints

docker exec -ti tca_flink-jobmanager flink run -p 1 -c de.prkz.twitch.emoteanalyser.EmoteAnalyser /analyser.jar \
	"jdbc:postgresql://db:5432/twitch?user=postgres&password=password" \
	"kafka:9092"
