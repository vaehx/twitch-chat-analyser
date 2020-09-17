package de.prkz.twitch.emoteanalyser.output;

import com.codahale.metrics.SlidingTimeWindowReservoir;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Writes batches of rows
 */
@Deprecated
public class BatchedPreparedDBOutputFormat extends RichOutputFormat<List<Row>> {

    private static final Logger LOG = LoggerFactory.getLogger(BatchedPreparedDBOutputFormat.class);

    private String driverClass;
    private String jdbcUrl;
    private String sql;
    private int[] typesArray;

    private transient Connection conn;
    private transient PreparedStatement stmt;

    private transient Histogram processBatchTime;
    private transient Histogram batchSize;
    private transient Counter batchesCompleted;

    @Override
    public void configure(Configuration configuration) {
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        try {
            Class.forName(driverClass);
            conn = DriverManager.getConnection(jdbcUrl);
            stmt = conn.prepareStatement(sql);
        } catch (Exception ex) {
            throw new IOException("Could not open db output format", ex);
        }

        MetricGroup metricGroup = getRuntimeContext().getMetricGroup();

        com.codahale.metrics.Histogram processBatchTimeHist =
                new com.codahale.metrics.Histogram(new SlidingTimeWindowReservoir(20, TimeUnit.SECONDS));
        processBatchTime = metricGroup.histogram("processBatchTime", new DropwizardHistogramWrapper(processBatchTimeHist));

        com.codahale.metrics.Histogram batchSizeHist =
                new com.codahale.metrics.Histogram(new SlidingTimeWindowReservoir(20, TimeUnit.SECONDS));
        batchSize = metricGroup.histogram("batchSize", new DropwizardHistogramWrapper(batchSizeHist));

        batchesCompleted = metricGroup.counter("batchesCompleted");
    }

    @Override
    public void writeRecord(List<Row> rows) throws IOException {
        if (rows == null || rows.isEmpty())
            return;

        long startTime = System.currentTimeMillis();

        try {
            for (Row row : rows)
                addToBatch(row);
        } catch (SQLException ex) {
            throw new IOException("Could not create batch from rows", ex);
        }

        try {
            stmt.executeBatch();
        } catch (SQLException ex) {
            throw new IOException("Could not execute batch", ex);
        }

        processBatchTime.update(System.currentTimeMillis() - startTime);
        batchSize.update(rows.size());
        batchesCompleted.inc();
    }

    private void addToBatch(Row row) throws SQLException {
        if (typesArray != null && typesArray.length > 0 && typesArray.length != row.getArity()) {
            LOG.warn("Column SQL types array doesn't match arity of passed Row! Check the passed array...");
        }

        if (typesArray == null) {
            // no types provided
            for (int index = 0; index < row.getArity(); index++) {
                LOG.warn("Unknown column type for column {}. Best effort approach to set its value: {}.", index + 1, row.getField(index));
                stmt.setObject(index + 1, row.getField(index));
            }
        } else {
            // types provided
            for (int index = 0; index < row.getArity(); index++) {
                if (row.getField(index) == null) {
                    stmt.setNull(index + 1, typesArray[index]);
                } else {
                    // casting values as suggested by http://docs.oracle.com/javase/1.5.0/docs/guide/jdbc/getstart/mapping.html
                    switch (typesArray[index]) {
                        case java.sql.Types.NULL:
                            stmt.setNull(index + 1, typesArray[index]);
                            break;
                        case java.sql.Types.BOOLEAN:
                        case java.sql.Types.BIT:
                            stmt.setBoolean(index + 1, (boolean) row.getField(index));
                            break;
                        case java.sql.Types.CHAR:
                        case java.sql.Types.NCHAR:
                        case java.sql.Types.VARCHAR:
                        case java.sql.Types.LONGVARCHAR:
                        case java.sql.Types.LONGNVARCHAR:
                            stmt.setString(index + 1, (String) row.getField(index));
                            break;
                        case java.sql.Types.TINYINT:
                            stmt.setByte(index + 1, (byte) row.getField(index));
                            break;
                        case java.sql.Types.SMALLINT:
                            stmt.setShort(index + 1, (short) row.getField(index));
                            break;
                        case java.sql.Types.INTEGER:
                            stmt.setInt(index + 1, (int) row.getField(index));
                            break;
                        case java.sql.Types.BIGINT:
                            stmt.setLong(index + 1, (long) row.getField(index));
                            break;
                        case java.sql.Types.REAL:
                            stmt.setFloat(index + 1, (float) row.getField(index));
                            break;
                        case java.sql.Types.FLOAT:
                        case java.sql.Types.DOUBLE:
                            stmt.setDouble(index + 1, (double) row.getField(index));
                            break;
                        case java.sql.Types.DECIMAL:
                        case java.sql.Types.NUMERIC:
                            stmt.setBigDecimal(index + 1, (java.math.BigDecimal) row.getField(index));
                            break;
                        case java.sql.Types.DATE:
                            stmt.setDate(index + 1, (java.sql.Date) row.getField(index));
                            break;
                        case java.sql.Types.TIME:
                            stmt.setTime(index + 1, (java.sql.Time) row.getField(index));
                            break;
                        case java.sql.Types.TIMESTAMP:
                            stmt.setTimestamp(index + 1, (java.sql.Timestamp) row.getField(index));
                            break;
                        case java.sql.Types.BINARY:
                        case java.sql.Types.VARBINARY:
                        case java.sql.Types.LONGVARBINARY:
                            stmt.setBytes(index + 1, (byte[]) row.getField(index));
                            break;
                        default:
                            stmt.setObject(index + 1, row.getField(index));
                            LOG.warn("Unmanaged sql type ({}) for column {}. Best effort approach to set its value: {}.",
                                    typesArray[index], index + 1, row.getField(index));
                            // case java.sql.Types.SQLXML
                            // case java.sql.Types.ARRAY:
                            // case java.sql.Types.JAVA_OBJECT:
                            // case java.sql.Types.BLOB:
                            // case java.sql.Types.CLOB:
                            // case java.sql.Types.NCLOB:
                            // case java.sql.Types.DATALINK:
                            // case java.sql.Types.DISTINCT:
                            // case java.sql.Types.OTHER:
                            // case java.sql.Types.REF:
                            // case java.sql.Types.ROWID:
                            // case java.sql.Types.STRUC
                    }
                }
            }
        }

        stmt.addBatch();
    }

    @Override
    public void close() throws IOException {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (Exception ex) {
                throw new IOException("Could not close statement", ex);
            } finally {
                stmt = null;
            }
        }

        if (conn != null) {
            try {
                conn.close();
            } catch (Exception ex) {
                throw new IOException("Could not properly close jdbc connection", ex);
            } finally {
                conn = null;
            }
        }
    }

    public static BatchedPreparedDBOutputFormatBuilder builder() {
        return new BatchedPreparedDBOutputFormatBuilder();
    }

    public static class BatchedPreparedDBOutputFormatBuilder {
        BatchedPreparedDBOutputFormat format = new BatchedPreparedDBOutputFormat();

        public BatchedPreparedDBOutputFormatBuilder withDriverClass(String driverClass) {
            format.driverClass = driverClass;
            return this;
        }

        public BatchedPreparedDBOutputFormatBuilder withJdbcUrl(String url) {
            format.jdbcUrl = url;
            return this;
        }

        public BatchedPreparedDBOutputFormatBuilder withSql(String sql) {
            format.sql = sql;
            return this;
        }

        public BatchedPreparedDBOutputFormatBuilder withTypesArray(int[] types) {
            format.typesArray = types;
            return this;
        }

        public BatchedPreparedDBOutputFormat finish() throws Exception {
            if (format.driverClass == null)
                throw new Exception("No driver class given");

            if (format.jdbcUrl == null)
                throw new Exception("No jdbc url given");

            if (format.sql == null)
                throw new Exception("No sql given");

            return format;
        }
    }
}
