package de.prkz

import de.prkz.hbase.sql.HBaseStreamSinkProvider
import org.apache.hadoop.hbase.client.{ConnectionFactory, HBaseAdmin}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.spark.sql.SparkSession

import scala.util.parsing.json.JSON

object Analyser {

	val HBASE_ZOOKEEPER_QUORUM = "docker-host"
	val HBASE_ZOOKEEPER_PORT = "2222"
	val HBASE_TABLE_NAME = "messages"

	val CF_METRICS = Bytes.toBytes("metrics")

	def main(args: Array[String]) {

		initDatabase()

		val spark = SparkSession
				.builder
				.appName("TwitchChatAnalyser")
				.config("spark.sql.streaming.checkpointLocation", "checkpoint")
				.getOrCreate()

		import spark.implicits._

		val rawMessages = spark
				.readStream
				.format("kafka")
				.option("kafka.bootstrap.servers", "docker-host:9092")
				.option("startingOffsets", "earliest")
				.option("subscribe", "twitch-chat")
				.load()
				.selectExpr("CAST(value AS STRING)")
				.as[String]

		val messages = rawMessages
				.map(json => {
					val m = JSON.parseFull(json).get.asInstanceOf[Map[String, String]]
					(m("channel"), m("text"))
				})

		val wordCounts = messages
				.flatMap(m => m._2.split("\\s+").map(w => (m._1, w)))
				.toDF("channel", "word")
				.groupBy("channel", "word")
				.count()

		val query = wordCounts
				.writeStream
				.outputMode("update")
				.format(classOf[HBaseStreamSinkProvider].getCanonicalName)
				.option("hbase.zookeeper.quorum", HBASE_ZOOKEEPER_QUORUM)
				.option("hbase.zookeeper.port", HBASE_ZOOKEEPER_PORT)
				.option("hbase.table", HBASE_TABLE_NAME)
				.option("hbase.rowconverter", classOf[MessageRowConverter].getCanonicalName)
				.start()

		query.awaitTermination()
	}

	private def initDatabase(): Unit = {
		val hbaseConf = HBaseConfiguration.create()
		hbaseConf.set("hbase.zookeeper.quorum", HBASE_ZOOKEEPER_QUORUM)
		hbaseConf.set("hbase.zookeeper.property.clientPort", HBASE_ZOOKEEPER_PORT)

		HBaseAdmin.checkHBaseAvailable(hbaseConf)

		val hbaseConn = ConnectionFactory.createConnection(hbaseConf)
		val hbaseAdmin = hbaseConn.getAdmin

		// Create table if not existing yet
		val tableName = TableName.valueOf(HBASE_TABLE_NAME)
		if (!hbaseAdmin.tableExists(tableName)) {
			val tableDesc = new HTableDescriptor(tableName)
			tableDesc.addFamily(new HColumnDescriptor(CF_METRICS))
			hbaseAdmin.createTable(tableDesc)
		}
	}

}
