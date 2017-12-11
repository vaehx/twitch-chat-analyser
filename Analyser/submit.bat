@echo off

spark-submit --master local[*]^
	--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.1^
	--class de.prkz.Analyser^
	target\twitch-analyser-1.0-jar-with-dependencies.jar
