package de.prkz

import org.apache.spark.sql.SparkSession

object Analyser {

	def main(args: Array[String]) {

		val spark = SparkSession
				.builder
				.appName("TwitchChatAnalyser")
				.getOrCreate()

		import spark.implicits._

		val messages = spark
				.readStream
				.format("kafka")
				.option("kafka.bootstrap.servers", "docker-host:9092")
				.option("startingOffsets", "earliest")
				.option("subscribe", "twitch-chat")
				.load()
				.selectExpr("CAST(value AS STRING)")
				.as[String]

		val wordCounts = messages
				.flatMap(_.split("\\s+"))
				.groupBy("value")
				.count()

		val query = wordCounts
				.as[(String, Int)]
				.writeStream
				.outputMode("update")
				.foreach(new HBaseForeachWriter)
				.start()

		query.awaitTermination()
	}

}
