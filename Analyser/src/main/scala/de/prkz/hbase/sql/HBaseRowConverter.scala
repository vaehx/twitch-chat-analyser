package de.prkz.hbase.sql

import org.apache.hadoop.hbase.client.Put
import org.apache.spark.sql.Row

trait HBaseRowConverter {
	def convertToPuts(row: Row): Array[Put]
}
