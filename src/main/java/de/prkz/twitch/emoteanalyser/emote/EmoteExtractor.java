package de.prkz.twitch.emoteanalyser.emote;

import de.prkz.twitch.emoteanalyser.Message;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.HashSet;
import java.util.Set;

public class EmoteExtractor extends RichFlatMapFunction<Message, Emote> {

	private static final Logger LOG = LoggerFactory.getLogger(EmoteExtractor.class);

	private static final String EMOTE_TABLE_NAME = "emotes";
	private static final long EMOTE_TABLE_TIMEOUT_MS = 60000;
	private transient long lastEmoteFetch;
	private transient Set<String> emotes;

	private String jdbcUrl;

	public EmoteExtractor(String jdbcUrl) {
		this.jdbcUrl = jdbcUrl;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		emotes = new HashSet<>();
		fetchEmotes();
		lastEmoteFetch = System.currentTimeMillis();

		for (String emote : emotes)
			LOG.info(emote);
	}

	@Override
	public void flatMap(Message message, Collector<Emote> collector) throws Exception {
		long time = System.currentTimeMillis();
		if (time - lastEmoteFetch > EMOTE_TABLE_TIMEOUT_MS) {
			fetchEmotes();
			lastEmoteFetch = time;
		}

		String[] words = message.message.split("\\s+");
		for (String word : words) {
			if (emotes.contains(word)) {
				Emote e = new Emote();
				e.timestamp = message.timestamp;
				e.channel = message.channel;
				e.emote = word;
				e.username = message.username;
				collector.collect(e);
			}
		}
	}

	private void fetchEmotes() throws Exception {
		emotes.clear();

		Connection conn = DriverManager.getConnection(jdbcUrl);
		Statement stmt = conn.createStatement();
		ResultSet result = stmt.executeQuery("SELECT emote FROM " + EMOTE_TABLE_NAME);
		while (result.next()) {
			emotes.add(result.getString(1));
		}

		LOG.info("Updated emote table. Now using " + emotes.size() + " emotes.");

		conn.close();
	}

	public void prepareTable(Statement stmt) throws SQLException {
		/*
			Emote type:
				0 - Twitch User
				1 - Twitch Global
				2 - BTTV
				3 - FFZ
				4 - Emoji
		 */
		stmt.execute("CREATE TABLE IF NOT EXISTS " + EMOTE_TABLE_NAME + "(" +
				"emote VARCHAR(64) NOT NULL," +
				"type SMALLINT NOT NULL DEFAULT 0," +
				"PRIMARY KEY(emote))");

		ResultSet emoteCountResult = stmt.executeQuery("SELECT COUNT(emote) FROM " + EMOTE_TABLE_NAME);
		emoteCountResult.next();
		if (emoteCountResult.getInt(1) == 0) {
			// Insert some default emotes, so we have something to track
			stmt.execute("INSERT INTO emotes(emote, type) VALUES('Kappa', 1), ('lirikN', 0), ('moon2S', 0);");
		}
	}
}
