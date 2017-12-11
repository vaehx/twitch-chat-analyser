package de.prkz.hbase.sql

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.streaming.Sink

@SerialVersionUID(1728L)
class HBaseSink(options: Map[String, String]) extends Sink with Logging with Serializable {

	private val zookeeperQuorum = options.get("hbase.zookeeper.quorum")
	private val zookeeperPort = options.get("hbase.zookeeper.port")
	private val tableName = options.get("hbase.table")
	private val rowConverter = createRowConverter(options("hbase.rowconverter"))

	private var lastBatchId = -1L

	private def createHBaseConnection(): Configuration = {
		val hbaseConf = HBaseConfiguration.create()
		hbaseConf.set("hbase.zookeeper.quorum", zookeeperQuorum.get)
		hbaseConf.set("hbase.zookeeper.property.clientPort", zookeeperPort.get)

		val hbaseWriterJob = Job.getInstance()
		hbaseWriterJob.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

		val hbaseWriterJobConf = hbaseWriterJob.getConfiguration
		hbaseWriterJobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName.get)

		hbaseWriterJobConf
	}

	private def createRowConverter(className: String): HBaseRowConverter = {
		Class.forName(className).newInstance.asInstanceOf[HBaseRowConverter]
	}

	override def addBatch(batchId: Long, data: DataFrame): Unit = synchronized {
		if (batchId <= lastBatchId)
			return

		data.queryExecution.toRdd.foreachPartition { iter =>
			val conn = HBaseSink.getHBaseConnection(zookeeperQuorum.get, zookeeperPort.get)
			val table = conn.getTable(TableName.valueOf(tableName.get))
			val mutator = conn.getBufferedMutator(TableName.valueOf(tableName.get))

			iter.foreach(row => {
				rowConverter.convertToPuts(row)
						.foreach(mutator.mutate(_))
			})

			table.close()
			mutator.flush()
			mutator.close()
		}

		lastBatchId = batchId
	}
}

object HBaseSink {
	@transient private var hbaseConn: Option[Connection] = None

	private def getHBaseConnection(zkQuorum: String,
								   zkPort: String): Connection = {
		hbaseConn.getOrElse {
			val conf = HBaseConfiguration.create()
			conf.set("hbase.zookeeper.quorum", zkQuorum)
			conf.set("hbase.zookeeper.property.clientPort", zkPort)

			val conn = ConnectionFactory.createConnection(conf)
			hbaseConn = Some(conn)

			conn
		}
	}
}
