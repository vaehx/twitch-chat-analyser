#!/bin/bash

docker cp ./target/emote-analyzer-1.0-SNAPSHOT.jar flink-jobmanager:/analyser.jar

docker exec -u root flink-jobmanager mkdir /data/checkpoints
docker exec -u root flink-jobmanager chown flink:flink /data/checkpoints

docker exec -u root flink-taskmanager mkdir /data/checkpoints
docker exec -u root flink-taskmanager chown flink:flink /data/checkpoints

docker exec -ti flink-jobmanager flink run -p 1 -c de.prkz.twitch.emoteanalyser.EmoteAnalyser /analyser.jar \
	"jdbc:postgresql://db:5432/twitch?user=postgres&password=password" \
	"kafka:9092"
