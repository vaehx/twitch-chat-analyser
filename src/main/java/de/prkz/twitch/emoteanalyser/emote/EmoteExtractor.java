package de.prkz.twitch.emoteanalyser.emote;

import de.prkz.twitch.emoteanalyser.Message;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class EmoteExtractor extends RichFlatMapFunction<Message, Emote> {

	private static final Logger LOG = LoggerFactory.getLogger(EmoteExtractor.class);

	private static final String EMOTES_TABLE_NAME = "emotes";
	private static final String TWITCH_API_CLIENT_ID = "ccxk8gzqpe0qd8t5lmwf45t1kplfi1";
	private static final long EMOTE_REFRESH_INTERVAL_MS = 5 * 60 * 1000;
	private transient long lastEmoteFetch;
	private transient Set<String> knownChannels;
	private transient Set<String> emotes;

	private String jdbcUrl;

	public EmoteExtractor(String jdbcUrl) {
		this.jdbcUrl = jdbcUrl;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		emotes = new HashSet<>();
		knownChannels = new HashSet<>();
		reloadEmotes();
		lastEmoteFetch = System.currentTimeMillis();
	}

	@Override
	public void flatMap(Message message, Collector<Emote> collector) throws Exception {
		long time = System.currentTimeMillis();
		if (!knownChannels.contains(message.channel)) {
			knownChannels.add(message.channel);
			reloadEmotes();
			lastEmoteFetch = time;
		} else if (time - lastEmoteFetch > EMOTE_REFRESH_INTERVAL_MS) {
			reloadEmotes();
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

	private static List<String> fetchChannelEmotes(String channel) throws IOException {
		URL url = new URL("https://api.twitch.tv/api/channels/" + channel + "/product" +
				"?client_id=" + TWITCH_API_CLIENT_ID);
		HttpURLConnection con = (HttpURLConnection)url.openConnection();
		con.setRequestMethod("GET");
		con.setRequestProperty("Content-Type", "application/json");

		int status = con.getResponseCode();
		if (status != 200)
			return null;

		BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
		String line;
		StringBuilder response = new StringBuilder();
		while ((line = in.readLine()) != null)
			response.append(line);

		in.close();
		con.disconnect();

		JSONObject responseObj = new JSONObject(response.toString());
		JSONArray emoticons = responseObj.getJSONArray("emoticons");
		if (emoticons == null)
			return null;

		List<String> emotes = new ArrayList<>();
		for (int i = 0; i < emoticons.length(); ++i) {
			// Add emote if not yet in table
			JSONObject emote = emoticons.getJSONObject(i);
			emotes.add(emote.getString("regex"));
		}

		return emotes;
	}

	private void reloadEmotes() throws Exception {
		Connection conn = DriverManager.getConnection(jdbcUrl);
		Statement stmt = conn.createStatement();

		// Fetch known channels from database (for initial load or manual table change)
		ResultSet channelsResult = stmt.executeQuery(
				"SELECT DISTINCT channel FROM " + EMOTES_TABLE_NAME + " WHERE channel IS NOT NULL");
		while (channelsResult.next()) {
			knownChannels.add(channelsResult.getString(1));
		}

		// Refresh Twitch Subscriber channel emotes in database (automatically add new ones)
		for (String channel : knownChannels) {
			List<String> emotes = fetchChannelEmotes(channel);
			if (emotes == null)
				continue;

			for (String emote : emotes) {
				stmt.execute("INSERT INTO " + EMOTES_TABLE_NAME + "(emote, type, channel) " +
						"VALUES('" + emote + "', 0, '" + channel + "') " +
						"ON CONFLICT(emote) DO NOTHING");
			}
		}

		// TODO: Also fetch BTTV and FFZ emotes ...

		// Re-fetch all emotes from table
		emotes.clear();

		ResultSet result = stmt.executeQuery("SELECT emote FROM " + EMOTES_TABLE_NAME);
		while (result.next()) {
			emotes.add(result.getString(1));
		}

		LOG.info("Updated emote table. Now using " + emotes.size() + " emotes.");

		conn.close();
	}

	public void prepareTables(Statement stmt) throws SQLException {
		/*
			Emote type:
				0 - Twitch channel subscriber
				1 - Twitch global
				2 - BTTV
				3 - FFZ
				4 - Emoji
		 */
		stmt.execute("CREATE TABLE IF NOT EXISTS " + EMOTES_TABLE_NAME + "(" +
				"emote VARCHAR NOT NULL," +
				"type SMALLINT NOT NULL DEFAULT 0," +
				"channel VARCHAR," + // if twitch channel subscriber emote
				"PRIMARY KEY(emote))");

		// If table is empty, insert some default emotes, so we have something to track
		ResultSet emoteCountResult = stmt.executeQuery("SELECT COUNT(emote) FROM " + EMOTES_TABLE_NAME);
		emoteCountResult.next();
		if (emoteCountResult.getInt(1) == 0) {
			stmt.execute("INSERT INTO emotes(emote, type) VALUES('Kappa', 1), ('PogChamp', 1), ('DansGame', 1);");
		}
	}
}
