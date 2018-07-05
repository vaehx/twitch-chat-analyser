package de.prkz.twitch.emoteanalyser.channel;

import de.prkz.twitch.emoteanalyser.AbstractStatsAggregation;
import de.prkz.twitch.emoteanalyser.Message;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Row;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

public class ChannelStatsAggregation extends AbstractStatsAggregation<Message, String, ChannelStats> {

	private static final String TABLE_NAME = "channel_stats";

	public ChannelStatsAggregation(String jdbcUrl, Time aggregationInterval) {
		super(jdbcUrl, aggregationInterval);
	}

	@Override
	protected ChannelStats createNewStatsForKey(String channel) throws SQLException {
		ChannelStats stats = new ChannelStats();
		stats.channel = channel;

		// Load current count from database, if it exists
		Statement stmt = conn.createStatement();
		ResultSet result = stmt.executeQuery("SELECT message_count FROM " + TABLE_NAME + " " +
				"WHERE channel='" + channel + "' " +
				"ORDER BY timestamp DESC LIMIT 1");

		if (result.next())
			stats.messageCount = result.getLong(1);
		else
			stats.messageCount = 0;

		stmt.close();
		return stats;
	}

	@Override
	protected ChannelStats updateStats(ChannelStats stats, Message message) {
		stats.messageCount++;
		return stats;
	}

	@Override
	public void prepareTable(Statement stmt) throws SQLException {
		stmt.execute("CREATE TABLE IF NOT EXISTS " + TABLE_NAME + "(" +
				"channel VARCHAR(32) NOT NULL," +
				"timestamp BIGINT NOT NULL," +
				"message_count BIGINT NOT NULL," +
				"PRIMARY KEY(channel, timestamp))");
	}

	@Override
	protected Row getRowFromStats(ChannelStats channelStats) {
		Row row = new Row(3);
		row.setField(0, channelStats.channel);
		row.setField(1, channelStats.timestamp);
		row.setField(2, channelStats.messageCount);
		return row;
	}

	@Override
	protected String getInsertSQL() {
		return "INSERT INTO " + TABLE_NAME + "(channel, timestamp, message_count) VALUES(?, ?, ?)";
	}

	@Override
	protected int[] getRowColumnTypes() {
		return new int[] { Types.VARCHAR, Types.BIGINT, Types.BIGINT };
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
