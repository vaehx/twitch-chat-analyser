package de.prkz.twitch.emoteanalyser.emote;

import de.prkz.twitch.emoteanalyser.AbstractStatsAggregation;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
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
	protected TypeInformation<EmoteStats> getStatsTypeInfo() {
		return TypeInformation.of(new TypeHint<EmoteStats>() {});
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
		ResultSet result = stmt.executeQuery("SELECT total_occurrences FROM " + TABLE_NAME + " " +
				"WHERE channel='" + stats.channel + "' AND emote='" + stats.emote + "' " +
				"ORDER BY timestamp DESC LIMIT 1");

		if (result.next())
			stats.totalOccurrences = result.getLong(1);
		else
			stats.totalOccurrences = 0;

		stmt.close();
		return stats;
	}

	@Override
	protected EmoteStats processWindowElements(EmoteStats stats, Iterable<Emote> emotes) {
		stats.occurrences = 0;
		for (Emote emote : emotes) {
			stats.occurrences++;
			stats.totalOccurrences++;
		}

		return stats;
	}

	@Override
	public void prepareTable(Statement stmt) throws SQLException {
		stmt.execute("CREATE TABLE IF NOT EXISTS " + TABLE_NAME + "(" +
				"channel VARCHAR(32) NOT NULL," +
				"emote VARCHAR(64) NOT NULL," +
				"timestamp BIGINT NOT NULL," +
				"total_occurrences BIGINT NOT NULL DEFAULT 0," +
				"occurrences INT NOT NULL DEFAULT 0," +
				"PRIMARY KEY(emote, timestamp))");
	}

	@Override
	protected Row getRowFromStats(EmoteStats stats) {
		Row row = new Row(4);
		row.setField(0, stats.channel);
		row.setField(1, stats.emote);
		row.setField(2, stats.timestamp);
		row.setField(3, stats.totalOccurrences);
		row.setField(4, stats.occurrences);
		return row;
	}

	@Override
	protected String getInsertSQL() {
		return "INSERT INTO " + TABLE_NAME + "(channel, emote, timestamp, total_occurrences, occurrences) VALUES(?, ?, ?, ?, ?)";
	}

	@Override
	protected int[] getRowColumnTypes() {
		return new int[] {
				Types.VARCHAR, /* channel */
				Types.VARCHAR, /* emote */
				Types.BIGINT, /* timestamp */
				Types.BIGINT, /* total_occurrences */
				Types.INTEGER /* occurrences*/
		};
	}
}
