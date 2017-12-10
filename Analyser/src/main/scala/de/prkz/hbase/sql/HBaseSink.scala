package de.prkz.hbase.sql

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.streaming.Sink

class HBaseSink(options: Map[String, String]) extends Sink with Logging {

	private val zookeeperQuorum = options.get("hbase.zookeeper.quorum")
	private val zookeeperPort = options.get("hbase.zookeeper.port")
	private val tableName = options.get("hbase.table")
	private val rowConverter = createRowConverter(options("hbase.rowconverter"))

	private val hbaseConf = HBaseConfiguration.create()
	hbaseConf.set("hbase.zookeeper.quorum", zookeeperQuorum.get)
	hbaseConf.set("hbase.zookeeper.property.clientPort", zookeeperPort.get)

	private val hbaseWriterJob = Job.getInstance()
	hbaseWriterJob.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

	private val hbaseWriterJobConf = hbaseWriterJob.getConfiguration
	hbaseWriterJobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName.get)

	override def addBatch(batchId: Long, data: DataFrame): Unit = synchronized {
		data.rdd.flatMap(rowConverter.convertToPuts)
				.map((new ImmutableBytesWritable, _))
				.saveAsNewAPIHadoopDataset(hbaseWriterJobConf)
	}

	def createRowConverter(className: String): HBaseRowConverter = {
		Class.forName(className).newInstance.asInstanceOf[HBaseRowConverter]
	}
}
