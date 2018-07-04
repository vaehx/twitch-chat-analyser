#!/bin/bash

docker cp ./target/emote-analyzer-1.0-SNAPSHOT.jar flink-jobmanager:/analyser.jar

docker exec -ti flink-jobmanager flink run -p 4 -c de.prkz.twitch.emoteanalyser.EmoteAnalyser /analyser.jar
