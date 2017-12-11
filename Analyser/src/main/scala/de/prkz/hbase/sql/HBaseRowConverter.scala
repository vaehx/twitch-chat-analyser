package de.prkz.hbase.sql

import org.apache.hadoop.hbase.client.Put
import org.apache.spark.sql.catalyst.InternalRow

trait HBaseRowConverter {
	def convertToPuts(row: InternalRow): Array[Put]
}
