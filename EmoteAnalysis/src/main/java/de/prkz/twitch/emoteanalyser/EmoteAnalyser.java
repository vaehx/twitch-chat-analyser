package de.prkz.twitch.emoteanalyser;

import de.prkz.twitch.emoteanalyser.emotes.*;
import de.prkz.twitch.emoteanalyser.userstats.MessageCountAggregation;
import de.prkz.twitch.emoteanalyser.userstats.UserStats;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

public class EmoteAnalyser {

	private static final Logger LOG = LoggerFactory.getLogger(EmoteAnalyser.class);
	private static final String jdbcUrl = "jdbc:postgresql://db:5432/twitch?user=postgres&password=password";

	public static void main(String[] args) throws Exception {

		LOG.debug("test-debug");
		LOG.info("test-info");
		LOG.warn("test-warn");

		// Prepare output table
		LOG.info("Preparing output table...");
		try {
			prepareTables();
		}
		catch (Exception ex) {
			LOG.error("Could not initialize table", ex);
			System.exit(1);
		}

		// TODO: Configure checkpointing, so we don't lose the state

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		//env.getConfig().setLatencyTrackingInterval(5L);

		// Twitch chat bot source
		DataStream<Message> messages = env
				.addSource(new TwitchSource("moonmoon_ow"))
				.name("TwitchSource");


		// Extract general message counts for each user
		DataStream<UserStats> userStats = messages
				.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Message>() {
					@Override
					public long extractAscendingTimestamp(Message message) {
						return message.timestamp;
					}
				})
				.keyBy(new KeySelector<Message, String>() {
					@Override
					public String getKey(Message message) throws Exception {
						return message.username;
					}
				})
				.window(TumblingEventTimeWindows.of(Time.minutes(1)))
				.process(new MessageCountAggregation(jdbcUrl));

		// Write user stats to database
		JDBCOutputFormat userStatsOutputFormat = JDBCOutputFormat.buildJDBCOutputFormat()
				.setDrivername("org.postgresql.Driver")
				.setDBUrl(jdbcUrl)
				.setQuery("INSERT INTO users(username, timestamp, message_count) VALUES(?, ?, ?)")
				.setSqlTypes(new int[] { Types.VARCHAR, Types.BIGINT, Types.BIGINT })
				.setBatchInterval(1)
				.finish();

		userStats
				.map((MapFunction<UserStats, Row>) userStats1 -> {
					Row row = new Row(3);
					row.setField(0, userStats1.username);
					row.setField(1, userStats1.timestamp );
					row.setField(2, userStats1.messageCount);
					return row;
				})
				.writeUsingOutputFormat(userStatsOutputFormat);



		// Extract emotes from messages
		// Event-Timestamps from Source are preserved
		DataStream<Emote> emotes = messages
				.flatMap(new EmoteExtractor(jdbcUrl))
				.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Emote>() {
					@Override
					public long extractAscendingTimestamp(Emote emote) {
						return emote.timestamp;
					}
				})
				.name("ExtractEmotes");


		// User-Emote Statistics
		DataStream<UserEmoteOccurences> userEmoteOccurences = emotes
				.keyBy(new KeySelector<Emote, Tuple2<String, String>>() {
					@Override
					public Tuple2<String, String> getKey(Emote emote) throws Exception {
						return new Tuple2<>(emote.username, emote.emote);
					}
				})
				.window(TumblingEventTimeWindows.of(Time.minutes(1)))
				.process(new UserEmoteOccurenceAggregation(jdbcUrl));

		JDBCOutputFormat userEmoteStatsOutputFormat = JDBCOutputFormat.buildJDBCOutputFormat()
				.setDrivername("org.postgresql.Driver")
				.setDBUrl(jdbcUrl)
				.setQuery("INSERT INTO emotes(username, emote, timestamp, occurrences) VALUES(?, ?, ?, ?)")
				.setSqlTypes(new int[] { Types.VARCHAR, Types.VARCHAR, Types.BIGINT, Types.BIGINT })
				.setBatchInterval(1)
				.finish();

		userEmoteOccurences
				.map((MapFunction<UserEmoteOccurences, Row>) occurrences -> {
					Row row = new Row(4);
					row.setField(0, occurrences.username);
					row.setField(1, occurrences.emote);
					row.setField(2, occurrences.timestamp);
					row.setField(3, occurrences.occurrences);
					return row;
				})
				.writeUsingOutputFormat(userEmoteStatsOutputFormat);


		// Total emote statistics
		DataStream<EmoteOccurences> emoteOccurences = emotes
				.keyBy(new KeySelector<Emote, String>() {
					@Override
					public String getKey(Emote emote) throws Exception {
						return emote.emote;
					}
				})
				.window(TumblingEventTimeWindows.of(Time.minutes(1)))
				.process(new EmoteOccurenceAggregation(jdbcUrl));

		JDBCOutputFormat emoteStatsOutputFormat = JDBCOutputFormat.buildJDBCOutputFormat()
				.setDrivername("org.postgresql.Driver")
				.setDBUrl(jdbcUrl)
				.setQuery("INSERT INTO emote_totals(emote, timestamp, occurrences) VALUES(?, ?, ?)")
				.setSqlTypes(new int[] { Types.VARCHAR, Types.BIGINT, Types.BIGINT })
				.setBatchInterval(1)
				.finish();

		emoteOccurences
				.map((MapFunction<EmoteOccurences, Row>) occurrences -> {
					Row row = new Row(3);
					row.setField(0, occurrences.emote);
					row.setField(1, occurrences.timestamp);
					row.setField(2, occurrences.occurrences);
					return row;
				})
				.writeUsingOutputFormat(emoteStatsOutputFormat);


		env.execute("EmoteAnalysis");
	}

	static void prepareTables() throws SQLException {
		Connection conn = DriverManager.getConnection(jdbcUrl);
		Statement stmt = conn.createStatement();

		/*
			Emote type:
				0 - Twitch User
				1 - Twitch Global
				2 - BTTV
				3 - FFZ
				4 - Emoji
		 */
		stmt.execute("CREATE TABLE IF NOT EXISTS emote_table(" +
				"emote VARCHAR(64) NOT NULL," +
				"type SMALLINT NOT NULL DEFAULT 0," +
				"PRIMARY KEY(emote))");

		stmt.execute("CREATE TABLE IF NOT EXISTS emotes(" +
				"username VARCHAR(32) NOT NULL," +
				"emote VARCHAR(64) NOT NULL," +
				"timestamp BIGINT NOT NULL," +
				"occurrences BIGINT NOT NULL DEFAULT 0," +
				"PRIMARY KEY(username, emote, timestamp))");

		stmt.execute("CREATE TABLE IF NOT EXISTS emote_totals(" +
				"emote VARCHAR(64) NOT NULL," +
				"timestamp BIGINT NOT NULL," +
				"occurrences BIGINT NOT NULL DEFAULT 0," +
				"PRIMARY KEY(emote, timestamp))");

		stmt.execute("CREATE TABLE IF NOT EXISTS users(" +
				"username VARCHAR(32) NOT NULL," +
				"timestamp BIGINT NOT NULL," +
				"message_count BIGINT NOT NULL DEFAULT 0," +
				"PRIMARY KEY(username, timestamp))");

		stmt.close();
		conn.close();
	}
}
