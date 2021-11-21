package de.prkz.twitch.emoteanalyser.emote;

import de.prkz.twitch.emoteanalyser.Message;
import de.prkz.twitch.emoteanalyser.emote.provider.*;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

public class EmoteExtractor extends RichFlatMapFunction<Message, Emote> {

    private static final Logger LOG = LoggerFactory.getLogger(EmoteExtractor.class);

    private static final String EMOTES_TABLE_NAME = "emotes";
    private static final String CHANNELS_TABLE_NAME = "channels";
    private static final long EMOTE_REFRESH_INTERVAL_MS = 5 * 60 * 1000;
    private static final int EMOTE_FETCH_TIMEOUT_MS = 20 * 1000;
    private transient long lastEmoteFetch;
    private transient Set<String> emotes;

    private final String jdbcUrl;
    private final List<EmoteProvider> emoteProviders;


    public EmoteExtractor(String jdbcUrl, String twitchClientId) {
        this.jdbcUrl = jdbcUrl;
        this.emoteProviders = Arrays.asList(
                new TwitchEmoteProvider(twitchClientId, EMOTE_FETCH_TIMEOUT_MS),
                new BTTVEmoteProvider(EMOTE_FETCH_TIMEOUT_MS),
                new FFZEmoteProvider(EMOTE_FETCH_TIMEOUT_MS),
                new SevenTVEmoteProvider(EMOTE_FETCH_TIMEOUT_MS)
        );
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        emotes = new HashSet<>();
        reloadEmotes();
        lastEmoteFetch = System.currentTimeMillis();
    }

    @Override
    public void flatMap(Message message, Collector<Emote> collector) throws Exception {
        long time = System.currentTimeMillis();
        if (time - lastEmoteFetch > EMOTE_REFRESH_INTERVAL_MS) {
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
     * <p>First, refreshes/completes the current emotes list in the database by re-fetching channel
     * emotes from twitch api for all known channels (as stored in <code>knownChannels</code>
     * and/or known channels in emotes table).</p>
     *
     * <p>Then, the whole emote list from database is loaded as the new set of emotes to match against.</p>
     *
     * <p>If some emotes could not be fetched due to an exception, the error will be logged and execution
     * continues, such that the fetch can be retried later.</p>
     */
    private void reloadEmotes() throws Exception {
        long startTime = System.currentTimeMillis();

        Class.forName(org.postgresql.Driver.class.getCanonicalName());
        try (Connection conn = DriverManager.getConnection(jdbcUrl);
             Statement stmt = conn.createStatement()) {

            // Check for new global/common emotes
            for (EmoteProvider provider : emoteProviders) {
                EmoteFetchResult result;
                try {
                    result = provider.fetchGlobalEmotes();
                } catch (Exception e) {
                    LOG.error("Could not fetch global emotes from provider {}",
                            provider.getClass().getCanonicalName(), e);
                    continue;
                }

                LOG.info("Fetched {} global emotes from provider {}", result.getEmotes().size(),
                        provider.getClass().getCanonicalName());

                insertNewEmotes(conn, result.getEmotes(), result.getEmoteType(), null);
            }


            // Fetch tracked channels from DB
            List<Channel> channels = new ArrayList<>();
            try (ResultSet channelsResult =
                         stmt.executeQuery("SELECT channel, emote_set FROM " + CHANNELS_TABLE_NAME)) {
                while (channelsResult.next()) {
                    channels.add(new Channel(channelsResult.getString("channel"), channelsResult.getInt("emote_set")));
                }
            }

            // Check for new channel emotes
            for (Channel channel : channels) {
                for (EmoteProvider provider : emoteProviders) {
                    EmoteFetchResult result;
                    try {
                        result = provider.fetchChannelEmotes(channel);
                    } catch (Exception e) {
                        LOG.error("Could not fetch channel emotes from provider {}",
                                provider.getClass().getCanonicalName(), e);
                        continue;
                    }

                    LOG.info("Fetched {} emotes from provider {} in channel '{}'", result.getEmotes().size(),
                            provider.getClass().getCanonicalName(), channel.name);

                    insertNewEmotes(conn, result.getEmotes(), result.getEmoteType(), result.getChannel());
                }
            }

            // Re-fetch all emotes from table
            emotes.clear();

            try (ResultSet result = stmt.executeQuery("SELECT emote FROM " + EMOTES_TABLE_NAME)) {
                while (result.next()) {
                    emotes.add(result.getString(1));
                }
            }

            long duration = System.currentTimeMillis() - startTime;

            LOG.info("Updated emote table in {} ms. Now using {} emotes in {} known channels",
                    duration, emotes.size(), channels.size());
        }
    }

    private void insertNewEmotes(Connection conn, Collection<String> emotes, EmoteType type, Channel channel)
            throws Exception {
        String sql = "INSERT INTO " + EMOTES_TABLE_NAME + "(emote, type, channel) " +
                "VALUES(?, ?, ?) " +
                "ON CONFLICT(emote) DO NOTHING";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            for (String emote : emotes) {
                if (emote == null || emote.isEmpty())
                    continue;

                stmt.setString(1, emote);
                stmt.setShort(2, type.identifier());
                stmt.setString(3, (channel != null) ? channel.name : null);
                stmt.executeUpdate();
            }
        }
    }

    public void prepareTables(Statement stmt) throws SQLException {
        // For emote type, see EmoteType enum
        stmt.execute("CREATE TABLE IF NOT EXISTS " + EMOTES_TABLE_NAME + "(" +
                "emote VARCHAR NOT NULL," +
                "type SMALLINT NOT NULL DEFAULT 0," +
                "channel VARCHAR," + // null if global
                "PRIMARY KEY(emote))");

        // If table is empty, insert some default emotes, so we have something to track
        try (ResultSet emoteCountResult = stmt.executeQuery("SELECT COUNT(emote) FROM " + EMOTES_TABLE_NAME)) {
            emoteCountResult.next();
            if (emoteCountResult.getInt(1) == 0) {
                stmt.execute("INSERT INTO emotes(emote, type) VALUES('Kappa', 1), ('PogChamp', 1), ('DansGame', 1);");
            }
        }


        // Channels table
        stmt.execute("CREATE TABLE IF NOT EXISTS " + CHANNELS_TABLE_NAME + "(" +
                "channel VARCHAR NOT NULL," +
                "emote_set INT NOT NULL," +
                "hidden BOOLEAN NOT NULL DEFAULT false)");
    }
}
