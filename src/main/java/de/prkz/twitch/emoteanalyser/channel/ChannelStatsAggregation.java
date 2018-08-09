package de.prkz.twitch.emoteanalyser.channel;

import de.prkz.twitch.emoteanalyser.AbstractStatsAggregation;
import de.prkz.twitch.emoteanalyser.Message;
import de.prkz.twitch.emoteanalyser.output.OutputStatement;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class ChannelStatsAggregation extends AbstractStatsAggregation<Message, String, ChannelStats> {

	private static final String TABLE_NAME = "channel_stats";

	public ChannelStatsAggregation(String jdbcUrl, long aggregationIntervalMillis) {
		super(jdbcUrl, aggregationIntervalMillis);
	}

	@Override
	protected ChannelStats createNewStatsForKey(String channel) throws SQLException {
		ChannelStats stats = new ChannelStats();
		stats.channel = channel;

		// Load current count from database, if it exists
		Statement stmt = conn.createStatement();
		ResultSet result = stmt.executeQuery("SELECT total_messages FROM " + TABLE_NAME + " " +
				"WHERE channel='" + channel + "' " +
				"ORDER BY timestamp DESC LIMIT 1");

		if (result.next())
			stats.totalMessageCount = result.getLong(1);
		else
			stats.totalMessageCount = 0;

		stmt.close();
		return stats;
	}

	@Override
	protected void processWindowElements(ChannelStats stats, Iterable<Message> messages) {
		stats.messageCount = 0;
		for (Message message : messages) {
			stats.messageCount++;
			stats.totalMessageCount++;
		}
	}

	@Override
	public void prepareTable(Statement stmt) throws SQLException {
		stmt.execute("CREATE TABLE IF NOT EXISTS " + TABLE_NAME + "(" +
				"channel VARCHAR(32) NOT NULL," +
				"timestamp BIGINT NOT NULL," +
				"total_messages BIGINT NOT NULL," +
				"messages INT NOT NULL," +
				"PRIMARY KEY(channel, timestamp))");
	}

	@Override
	protected Iterable<OutputStatement> prepareStatsForOutput(ChannelStats stats) {
		return OutputStatement.buildBatch()
				.add("INSERT INTO " + TABLE_NAME + "(timestamp, channel, total_messages, messages) " +
						"VALUES(" + stats.timestamp + ", '" + stats.channel + "', " + stats.totalMessageCount + ", " + stats.messageCount + ")")
				.add("INSERT INTO " + TABLE_NAME + "(timestamp, channel, total_messages, messages) " +
						"VALUES(0, '" + stats.channel + "', " + stats.totalMessageCount + ", " + stats.messageCount + ") " +
						"ON CONFLICT(channel, timestamp) DO UPDATE " +
							"SET total_messages = excluded.total_messages, messages = excluded.messages")
				.finish();
	}

	@Override
	public void close() throws Exception {
		conn.close();
	}

	@Override
	protected TypeInformation<ChannelStats> getStatsTypeInfo() {
		return TypeInformation.of(new TypeHint<ChannelStats>() {});
	}

	@Override
	protected KeySelector<Message, String> createKeySelector() {
		return new KeySelector<Message, String>() {
			@Override
			public String getKey(Message message) throws Exception {
				return message.channel;
			}
		};
	}
}
