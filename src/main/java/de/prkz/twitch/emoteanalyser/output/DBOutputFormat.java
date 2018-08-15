package de.prkz.twitch.emoteanalyser.output;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBOutputFormat extends RichOutputFormat<OutputStatement> {

	private String driverClass;
	private String jdbcUrl;
	private int batchSize = 1;

	private transient Connection conn;
	private transient java.sql.Statement stmt;
	private transient int inCurrentBatch;

	@Override
	public void configure(Configuration configuration) {
	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		try {
			Class.forName(driverClass);
			conn = DriverManager.getConnection(jdbcUrl);
			stmt = conn.createStatement();
			inCurrentBatch = 0;
		}
		catch (Exception ex) {
			throw new IOException("Could not open db output format", ex);
		}
	}

	@Override
	public void writeRecord(OutputStatement statement) throws IOException {
		try {
			stmt.addBatch(statement.getSql());
			inCurrentBatch++;
		}
		catch (Exception ex) {
			throw new IOException("Could not execute statement " + statement.getSql(), ex);
		}

		if (inCurrentBatch >= batchSize)
			flush();
	}

	private void flush() throws IOException {
		try {
			stmt.executeBatch();
			inCurrentBatch = 0;
		}
		catch (SQLException ex) {
			throw new IOException("Could not execute batch", ex);
		}
	}

	@Override
	public void close() throws IOException {
		if (stmt != null) {
			if (inCurrentBatch > 0)
				flush();

			try {
				stmt.close();
			}
			catch (Exception ex) {
				throw new IOException("Could not close statement", ex);
			}
			finally {
				stmt = null;
			}
		}

		if (conn != null) {
			try {
				conn.close();
			}
			catch (Exception ex) {
				throw new IOException("Could not properly close jdbc connection", ex);
			}
			finally {
				conn = null;
			}
		}
	}

	public static DBOutputFormatBuilder buildDBOutputFormat() {
		return new DBOutputFormatBuilder();
	}

	public static class DBOutputFormatBuilder {
		DBOutputFormat format = new DBOutputFormat();

		public DBOutputFormatBuilder withDriverClass(String driverClass) {
			format.driverClass = driverClass;
			return this;
		}

		public DBOutputFormatBuilder withJdbcUrl(String url) {
			format.jdbcUrl = url;
			return this;
		}

		public DBOutputFormatBuilder withBatchSize(int batchSize) {
			format.batchSize = batchSize;
			return this;
		}

		public DBOutputFormat finish() throws Exception {
			if (format.driverClass == null)
				throw new Exception("No driver class given");

			if (format.jdbcUrl == null)
				throw new Exception("No jdbc url given");

			if (format.batchSize <= 0)
				throw new Exception("Invalid batch size of " + format.batchSize);

			return format;
		}
	}
}
