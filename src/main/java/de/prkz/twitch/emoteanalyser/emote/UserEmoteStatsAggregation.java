package de.prkz.twitch.emoteanalyser.emote;

import de.prkz.twitch.emoteanalyser.AbstractStatsAggregation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Row;

import java.sql.*;

// Key: (channel, emote, username)
public class UserEmoteStatsAggregation
		extends AbstractStatsAggregation<Emote, Tuple3<String, String, String>, UserEmoteStats> {

	private static final String TABLE_NAME = "user_emote_stats";

	public UserEmoteStatsAggregation(String jdbcUrl, Time aggregationInterval) {
		super(jdbcUrl, aggregationInterval);
	}

	@Override
	protected KeySelector<Emote, Tuple3<String, String, String>> createKeySelector() {
		return new KeySelector<Emote, Tuple3<String, String, String>>() {
			@Override
			public Tuple3<String, String, String> getKey(Emote emote) throws Exception {
				return new Tuple3<>(emote.channel, emote.emote, emote.username);
			}
		};
	}

	@Override
	protected UserEmoteStats createNewStatsForKey(Tuple3<String, String, String> key) throws SQLException {
		UserEmoteStats stats = new UserEmoteStats();
		stats.channel = key.f0;
		stats.emote = key.f1;
		stats.username = key.f2;

		// Load current count from database, if it exists
		Statement stmt = conn.createStatement();
		ResultSet result = stmt.executeQuery("SELECT occurrences FROM " + TABLE_NAME + " WHERE " +
				"channel='" + stats.channel + "' AND emote='" + stats.emote + "' AND username='" + stats.username + "' " +
				"ORDER BY timestamp DESC LIMIT 1");

		if (result.next())
			stats.occurrences = result.getLong(1);
		else
			stats.occurrences = 0;

		stmt.close();
		return stats;
	}

	@Override
	protected UserEmoteStats updateStats(UserEmoteStats stats, Emote emote) {
		stats.occurrences++;
		return stats;
	}

	@Override
	public void prepareTable(Statement stmt) throws SQLException {
		stmt.execute("CREATE TABLE IF NOT EXISTS " + TABLE_NAME + "(" +
				"channel VARCHAR(32) NOT NULL," +
				"emote VARCHAR(64) NOT NULL," +
				"username VARCHAR(32) NOT NULL," +
				"timestamp BIGINT NOT NULL," +
				"occurrences BIGINT NOT NULL DEFAULT 0," +
				"PRIMARY KEY(channel, emote, username, timestamp))");
	}

	@Override
	protected Row getRowFromStats(UserEmoteStats stats) {
		Row row = new Row(4);
		row.setField(0, stats.channel);
		row.setField(1, stats.emote);
		row.setField(2, stats.username);
		row.setField(3, stats.timestamp);
		row.setField(4, stats.occurrences);
		return row;
	}

	@Override
	protected String getInsertSQL() {
		return "INSERT INTO " + TABLE_NAME + "(channel, emote, username, timestamp, occurrences) VALUES(?, ?, ?, ?, ?)";
	}

	@Override
	protected int[] getRowColumnTypes() {
		return new int[] {
				Types.VARCHAR, /* channel */
				Types.VARCHAR, /* emote */
				Types.VARCHAR, /* username */
				Types.BIGINT, /* timestamp */
				Types.BIGINT /* occurrences */
		};
	}
}
