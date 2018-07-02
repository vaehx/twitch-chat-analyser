package de.prkz.twitch.emoteanalyser;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
			prepareTable();
		}
		catch (Exception ex) {
			LOG.error("Could not initialize table", ex);
			System.exit(1);
		}

		// TODO: Configure checkpointing, so we don't lose the state

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.getConfig().setLatencyTrackingInterval(5L);

		// Twitch chat bot source
		DataStream<Message> messages = env
				.addSource(new TwitchSource("moonmoon_ow"))
				.name("TwitchSource");

		// Extract words from messages
		// Event-Timestamps from Source are preserved
		DataStream<Emote> emotes = messages
				.flatMap(new EmoteExtractor())
				.name("ExtractEmotes");

		DataStream<EmoteOccurences> emoteOccurences = emotes
				.keyBy(new KeySelector<Emote, Tuple2<String, String>>() {
					@Override
					public Tuple2<String, String> getKey(Emote emote) throws Exception {
						return new Tuple2<>(emote.username, emote.emote);
					}
				})
				.window(TumblingEventTimeWindows.of(Time.minutes(1)))
				.process(new OccurenceAggregation());

		// Write occurence counts as timeseries to database
		JDBCOutputFormat jdbcOutputFormat = JDBCOutputFormat.buildJDBCOutputFormat()
				.setDrivername("org.postgresql.Driver")
				.setDBUrl(jdbcUrl)
				.setQuery("INSERT INTO emotes(username, emote, timestamp, occurrences) VALUES(?, ?, ?, ?)")
				.setSqlTypes(new int[] { Types.VARCHAR, Types.VARCHAR, Types.BIGINT, Types.BIGINT })
				.setBatchInterval(1)
				.finish();

		emoteOccurences
				.map((MapFunction<EmoteOccurences, Row>) occurrences -> {
					Row row = new Row(4);
					row.setField(0, occurrences.username);
					row.setField(1, occurrences.emote);
					row.setField(2, occurrences.timestamp);
					row.setField(3, occurrences.occurrences);
					return row;
				})
				.writeUsingOutputFormat(jdbcOutputFormat);

		env.execute("EmoteAnalysis");
	}

	static void prepareTable() throws SQLException {
		Connection conn = DriverManager.getConnection(jdbcUrl);
		Statement stmt = conn.createStatement();
		stmt.execute("CREATE TABLE IF NOT EXISTS emotes(" +
				"username VARCHAR(32) NOT NULL," +
				"emote VARCHAR(64) NOT NULL," +
				"timestamp BIGINT NOT NULL," +
				"occurrences BIGINT NOT NULL DEFAULT 0," +
				"PRIMARY KEY(username, emote, timestamp))");
		stmt.close();
		conn.close();
	}
}
