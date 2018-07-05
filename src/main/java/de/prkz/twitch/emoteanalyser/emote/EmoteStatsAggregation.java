package de.prkz.twitch.emoteanalyser.emote;

import de.prkz.twitch.emoteanalyser.AbstractStatsAggregation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Row;

import java.sql.*;

// Key: (channel, emote)
public class EmoteStatsAggregation
		extends AbstractStatsAggregation<Emote, Tuple2<String, String>, EmoteStats> {

	private static final String TABLE_NAME = "emote_stats";

	public EmoteStatsAggregation(String jdbcUrl, Time aggregationInterval) {
		super(jdbcUrl, aggregationInterval);
	}

	@Override
	protected KeySelector<Emote, Tuple2<String, String>> createKeySelector() {
		return new KeySelector<Emote, Tuple2<String, String>>() {
			@Override
			public Tuple2<String, String> getKey(Emote emote) throws Exception {
				return new Tuple2<>(emote.channel, emote.emote);
			}
		};
	}

	@Override
	protected EmoteStats createNewStatsForKey(Tuple2<String, String> key) throws SQLException {
		EmoteStats stats = new EmoteStats();
		stats.channel = key.f0;
		stats.emote = key.f1;

		// Load current count from database, if it exists
		Statement stmt = conn.createStatement();
		ResultSet result = stmt.executeQuery("SELECT occurrences FROM " + TABLE_NAME + " " +
				"WHERE channel='" + stats.channel + "' AND emote='" + stats.emote + "' " +
				"ORDER BY timestamp DESC LIMIT 1");

		if (result.next())
			stats.occurrences = result.getLong(1);
		else
			stats.occurrences = 0;

		stmt.close();
		return stats;
	}

	@Override
	protected EmoteStats updateStats(EmoteStats stats, Emote emote) {
		stats.occurrences++;
		return stats;
	}

	@Override
	public void prepareTable(Statement stmt) throws SQLException {
		stmt.execute("CREATE TABLE IF NOT EXISTS " + TABLE_NAME + "(" +
				"channel VARCHAR(32) NOT NULL," +
				"emote VARCHAR(64) NOT NULL," +
				"timestamp BIGINT NOT NULL," +
				"occurrences BIGINT NOT NULL DEFAULT 0," +
				"PRIMARY KEY(emote, timestamp))");
	}

	@Override
	protected Row getRowFromStats(EmoteStats stats) {
		Row row = new Row(4);
		row.setField(0, stats.channel);
		row.setField(1, stats.emote);
		row.setField(2, stats.timestamp);
		row.setField(3, stats.occurrences);
		return row;
	}

	@Override
	protected String getInsertSQL() {
		return "INSERT INTO " + TABLE_NAME + "(channel, emote, timestamp, occurrences) VALUES(?, ?, ?, ?)";
	}

	@Override
	protected int[] getRowColumnTypes() {
		return new int[] { Types.VARCHAR, Types.VARCHAR, Types.BIGINT, Types.BIGINT };
	}
}
