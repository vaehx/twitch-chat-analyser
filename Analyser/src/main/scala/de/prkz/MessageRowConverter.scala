package de.prkz

import de.prkz.hbase.sql.HBaseRowConverter
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow

@SerialVersionUID(8293L)
class MessageRowConverter extends HBaseRowConverter with Serializable {

	override def convertToPuts(row: InternalRow): Array[Put] = {
		val channel = row.getString(0)
		val word = row.getString(1)
		val count = row.getLong(2)

		val put = new Put(Bytes.toBytes(channel + "!" + word))
		put.addColumn(Analyser.CF_METRICS, Bytes.toBytes("occurrences"), Bytes.toBytes(count))

		Array(put)
	}
}
