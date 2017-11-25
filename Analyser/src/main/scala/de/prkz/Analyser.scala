package de.prkz

import org.apache.spark.sql.SparkSession

object Analyser {

	def main(args: Array[String]): Unit = {

		val spark = SparkSession
				.builder
				.appName("TwitchChatAnalyser")
				.getOrCreate()

		val messages = spark
				.readStream
				.format("kafa")
				.option("kafka.bootstrap.servers", "twitchbot-kafka")
				.option("subscribe", "twitch-chat")
				.load()
				.selectExpr("CAST(value AS STRING)")
				.as[String]

		val wordCounts = messages
				.flatMap(_.split("\s+"))
				.groupBy("value")
				.count()

		val query = wordCounts
				.writeStream
				.outputMode("complete")
				.format("console")
				.start()

	}

}
