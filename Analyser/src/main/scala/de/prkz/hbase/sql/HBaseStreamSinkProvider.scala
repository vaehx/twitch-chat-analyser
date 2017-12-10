package de.prkz.hbase.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.OutputMode

class HBaseStreamSinkProvider extends StreamSinkProvider with DataSourceRegister {

	def createSink(sqlContext: SQLContext,
				   parameters: Map[String, String],
				   partitionColumns: Seq[String],
				   outputMode: OutputMode): Sink = {
		new HBaseSink(parameters)
	}

	override def shortName(): String = "hbase"
}
