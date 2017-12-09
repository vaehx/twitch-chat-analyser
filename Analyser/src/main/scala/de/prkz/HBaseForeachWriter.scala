package de.prkz

import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.ForeachWriter

class HBaseForeachWriter extends ForeachWriter[(String, Int)] {

	var conn: Connection = null
	var tableName: TableName = TableName.valueOf("twitch-chat")

	val CF_METRICS = Bytes.toBytes("metrics")

	def open(partitionId: Long, version: Long): Boolean = {
		val conf = HBaseConfiguration.create
		conf.set("hbase.zookeeper.quorum", "docker-host")
		conf.set("hbase.zookeeper.property.clientPort", "2181")

		HBaseAdmin.checkHBaseAvailable(conf)

		conn = ConnectionFactory.createConnection(conf)

		val admin = conn.getAdmin
		if (!admin.tableExists(tableName)) {
			val tableDescriptor = new HTableDescriptor(tableName)
			tableDescriptor.addFamily(new HColumnDescriptor(CF_METRICS))

			admin.createTable(tableDescriptor)
		}

		true
	}

	def process(record: (String, Int)) = {
		val put = new Put(Bytes.toBytes(record._1))
		put.addColumn(CF_METRICS, Bytes.toBytes("occurences"), Bytes.toBytes(record._2))
	}

	def close(errorOrNull: Throwable): Unit = {
		conn.close()
	}
}