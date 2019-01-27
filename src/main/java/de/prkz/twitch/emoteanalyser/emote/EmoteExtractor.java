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
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.*;
import java.util.*;

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
            LOG.info("Found new channel '" + message.channel + "'. Refreshing emotes...");
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

    /**
     * First, refreshes/completes the current emotes list in the database by re-fetching channel
     * emotes from twitch api for all known channels (as stored in <code>knownChannels</code>
     * and/or known channels in emotes table).
     * <p>
     * Then, the whole emote list from database is loaded as the new set of emotes to match against.
     */
    private void reloadEmotes() throws Exception {
        long startTime = System.currentTimeMillis();

        Class.forName(org.postgresql.Driver.class.getCanonicalName());
        Connection conn = DriverManager.getConnection(jdbcUrl);
        Statement stmt = conn.createStatement();

        // Fetch known channels from database (for initial load or manual table change)
        ResultSet channelsResult = stmt.executeQuery(
                "SELECT DISTINCT channel FROM " + EMOTES_TABLE_NAME + " WHERE channel IS NOT NULL");
        while (channelsResult.next()) {
            knownChannels.add(channelsResult.getString(1));
        }

        // Check for new global twitch emotes
        try {
            Collection<String> emotes = fetchGlobalTwitchEmotes();
            insertNewEmotes(stmt, emotes, 1, null);
        } catch (Exception ex) {
            LOG.error("Could not fetch global twitch emotes: " + ex.getMessage());
        }

        // Check for new global BTTV emotes
        try {
            Collection<String> emotes = fetchGlobalBTTVEmotes();
            insertNewEmotes(stmt, emotes, 2, null);
        } catch (Exception ex) {
            LOG.error("Could not fetch global BTTV emotes: " + ex.getMessage());
        }

        // Check for new channel emotes
        for (String channel : knownChannels) {
            // Twitch subscriber emotes
            try {
                Collection<String> emotes = fetchSubscriberEmotes(channel);
                insertNewEmotes(stmt, emotes, 0, channel);
            } catch (Exception ex) {
                LOG.error("Could not fetch subscriber emotes for channel '" + channel + "': " + ex.getMessage());
            }

            // BTTV channel emotes
            try {
                Collection<String> emotes = fetchBTTVChannelEmotes(channel);
                insertNewEmotes(stmt, emotes, 2, channel);
            } catch (Exception ex) {
                LOG.error("Could not fetch BTTV emotes for channel '" + channel + "': " + ex.getMessage());
            }

            // FFZ channel emotes
            try {
                Collection<String> emotes = fetchFFZChannelEmotes(channel);
                insertNewEmotes(stmt, emotes, 3, channel);
            } catch (Exception ex) {
                LOG.error("Could not fetch FFZ emotes for channel '" + channel + "': " + ex.getMessage());
            }
        }

        // Re-fetch all emotes from table
        emotes.clear();

        ResultSet result = stmt.executeQuery("SELECT emote FROM " + EMOTES_TABLE_NAME);
        while (result.next()) {
            emotes.add(result.getString(1));
        }

        long duration = System.currentTimeMillis() - startTime;

        LOG.info("Updated emote table in {} ms. Now using {} emotes in {} known channels",
                duration, emotes.size(), knownChannels.size());

        conn.close();
    }

    private void insertNewEmotes(Statement stmt, Collection<String> emotes, int type, String channel) throws Exception {
        String channelValue = (channel != null) ? "'" + channel + "'" : "NULL";

        for (String emote : emotes) {
            if (emote == null || emote.isEmpty())
                continue;

            String sanitizedEmote = emote.replaceAll("'", "''");

            stmt.execute("INSERT INTO " + EMOTES_TABLE_NAME + "(emote, type, channel) " +
                    "VALUES('" + sanitizedEmote + "', " + type + ", " + channelValue + ") " +
                    "ON CONFLICT(emote) DO NOTHING");
        }
    }

    private static Collection<String> fetchGlobalTwitchEmotes() throws Exception {
        URL url = new URL("https://twitchemotes.com/api_cache/v3/global.json");
        String response = getJSONHttp(url);

        JSONObject responseObj = new JSONObject(response);
        List<String> emotes = new ArrayList<>(responseObj.keySet());

        LOG.info("Fetched " + emotes.size() + " global twitch emotes");

        return emotes;
    }

    private static Collection<String> fetchSubscriberEmotes(String channel) throws Exception {
        URL url = new URL("https://api.twitch.tv/api/channels/" + channel + "/product" +
                "?client_id=" + TWITCH_API_CLIENT_ID);
        String response = getJSONHttp(url);

        JSONObject responseObj = new JSONObject(response);
        JSONArray emoticons = responseObj.getJSONArray("emoticons");
        if (emoticons == null)
            throw new Exception("Response does not include emoticons");

        List<String> emotes = new ArrayList<>();
        for (int i = 0; i < emoticons.length(); ++i) {
            JSONObject emote = emoticons.getJSONObject(i);
            emotes.add(emote.getString("regex"));
        }

        LOG.info("Fetched " + emotes.size() + " subscriber emotes for channel '" + channel + "'");

        return emotes;
    }

    private static Collection<String> fetchGlobalBTTVEmotes() throws Exception {
        URL url = new URL("https://api.betterttv.net/2/emotes");
        String response = getJSONHttp(url);

        Collection<String> emotes = parseBTTVEmotesResponse(response);

        LOG.info("Fetched " + emotes.size() + " global BTTV emotes");

        return emotes;
    }

    private static Collection<String> fetchBTTVChannelEmotes(String channel) throws Exception {
        URL url = new URL("https://api.betterttv.net/2/channels/" + channel);
        String response = getJSONHttp(url);

        Collection<String> emotes = parseBTTVEmotesResponse(response);

        LOG.info("Fetched " + emotes.size() + " BTTV emotes in channel '" + channel + "'");

        return emotes;
    }

    private static Collection<String> parseBTTVEmotesResponse(String response) throws Exception {
        JSONObject responseObj = new JSONObject(response);
        JSONArray emotesArr = responseObj.getJSONArray("emotes");
        if (emotesArr == null)
            throw new Exception("Response does not include emotes array");

        List<String> emotes = new ArrayList<>();
        for (int i = 0; i < emotesArr.length(); ++i) {
            JSONObject emoteObj = emotesArr.getJSONObject(i);
            emotes.add(emoteObj.getString("code"));
        }

        return emotes;
    }

    private static Collection<String> fetchFFZChannelEmotes(String channel) throws Exception {
        URL url = new URL("https://api.frankerfacez.com/v1/room/" + channel);
        String response = getJSONHttp(url);

        List<String> emotes = new ArrayList<>();

        JSONObject responseObj = new JSONObject(response);
        JSONObject sets = responseObj.getJSONObject("sets");
        if (sets == null)
            throw new Exception("Invalid response: 'sets' object missing");

        for (String setId : sets.keySet()) {
            JSONObject setObj = sets.getJSONObject(setId);
            JSONArray emoticons = setObj.getJSONArray("emoticons");
            if (emoticons == null)
                continue;

            for (int i = 0; i < emoticons.length(); ++i) {
                JSONObject emoticon = emoticons.getJSONObject(i);
                emotes.add(emoticon.getString("name"));
            }
        }

        LOG.info("Fetched " + emotes.size() + " FFZ emotes for channel '" + channel + "'");

        return emotes;
    }

    private static String getJSONHttp(URL url) throws Exception {
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod("GET");
        con.setRequestProperty("Content-Type", "application/json");

        int status = con.getResponseCode();
        if (status == 404) {
            throw new Exception("Not found (404)");
        } else if (status != 200) {
            throw new Exception("Got HTTP error for request to URL '" + url.toString() + "': " +
                    "Code " + con.getResponseCode() + ", Message: " + con.getResponseMessage());
        }

        BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
        String line;
        StringBuilder response = new StringBuilder();
        while ((line = in.readLine()) != null)
            response.append(line);

        in.close();
        con.disconnect();

        return response.toString();
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
                "channel VARCHAR," + // null if global
                "PRIMARY KEY(emote))");

        // If table is empty, insert some default emotes, so we have something to track
        ResultSet emoteCountResult = stmt.executeQuery("SELECT COUNT(emote) FROM " + EMOTES_TABLE_NAME);
        emoteCountResult.next();
        if (emoteCountResult.getInt(1) == 0) {
            stmt.execute("INSERT INTO emotes(emote, type) VALUES('Kappa', 1), ('PogChamp', 1), ('DansGame', 1);");
        }
    }
}
