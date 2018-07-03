package de.prkz.twitch.emoteanalyser.emotes;

import de.prkz.twitch.emoteanalyser.Message;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

public class EmoteExtractor extends RichFlatMapFunction<Message, Emote> {

	private static final Logger LOG = LoggerFactory.getLogger(EmoteExtractor.class);

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
		ResultSet result = stmt.executeQuery("SELECT emote FROM emote_table");
		while (result.next()) {
			emotes.add(result.getString(1));
		}

		LOG.info("Updated emote table. Now using " + emotes.size() + " emotes.");

		conn.close();
	}
}
