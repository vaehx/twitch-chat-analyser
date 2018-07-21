package de.prkz.twitch.emoteanalyser.user;

import de.prkz.twitch.emoteanalyser.AbstractStatsAggregation;
import de.prkz.twitch.emoteanalyser.Message;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Row;

import java.sql.*;

public class UserStatsAggregation
		extends AbstractStatsAggregation<Message, Tuple2<String, String>, UserStats> {

	private static final String TABLE_NAME = "user_stats";

	public UserStatsAggregation(String jdbcUrl, Time aggregationInterval) {
		super(jdbcUrl, aggregationInterval);
	}

	@Override
	protected TypeInformation<UserStats> getStatsTypeInfo() {
		return TypeInformation.of(new TypeHint<UserStats>() {});
	}

	@Override
	protected KeySelector<Message, Tuple2<String, String>> createKeySelector() {
		return new KeySelector<Message, Tuple2<String, String>>() {
			@Override
			public Tuple2<String, String> getKey(Message message) throws Exception {
				return new Tuple2<>(message.channel, message.username);
			}
		};
	}

	@Override
	protected UserStats createNewStatsForKey(Tuple2<String, String> key) throws SQLException {
		UserStats stats = new UserStats();
		stats.channel = key.f0;
		stats.username = key.f1;

		// Load current count from database, if it exists
		Statement stmt = conn.createStatement();
		ResultSet result = stmt.executeQuery("SELECT message_count FROM " + TABLE_NAME + " " +
				"WHERE channel='" + stats.channel + "' AND username='" + stats.username + "' " +
				"ORDER BY timestamp DESC LIMIT 1");

		if (result.next())
			stats.totalMessageCount = result.getLong(1);
		else
			stats.totalMessageCount = 0;

		stmt.close();
		return stats;
	}

	@Override
	protected UserStats processWindowElements(UserStats stats, Iterable<Message> messages) {
		stats.messageCount = 0;
		for (Message message : messages) {
			stats.messageCount++;
			stats.totalMessageCount++;
		}

		return stats;
	}

	@Override
	public void prepareTable(Statement stmt) throws SQLException {
		stmt.execute("CREATE TABLE IF NOT EXISTS " + TABLE_NAME + "(" +
				"channel VARCHAR(32) NOT NULL," +
				"username VARCHAR(32) NOT NULL," +
				"timestamp BIGINT NOT NULL," +
				"total_messages BIGINT NOT NULL DEFAULT 0," +
				"messages INT NOT NULL DEFAULT 0," +
				"PRIMARY KEY(channel, username, timestamp))");
	}

	@Override
	protected Row getRowFromStats(UserStats stats) {
		Row row = new Row(4);
		row.setField(0, stats.channel);
		row.setField(1, stats.username);
		row.setField(2, stats.timestamp);
		row.setField(3, stats.totalMessageCount);
		row.setField(4, stats.messageCount);
		return row;
	}

	@Override
	protected String getInsertSQL() {
		return "INSERT INTO " + TABLE_NAME + "(channel, username, timestamp, total_messages, messages) VALUES(?, ?, ?, ?, ?)";
	}

	@Override
	protected int[] getRowColumnTypes() {
		return new int[] {
				Types.VARCHAR, /* channel */
				Types.VARCHAR, /* username */
				Types.BIGINT, /* timestamp */
				Types.BIGINT, /* total_messages */
				Types.INTEGER /* messages */
		};
	}
}
