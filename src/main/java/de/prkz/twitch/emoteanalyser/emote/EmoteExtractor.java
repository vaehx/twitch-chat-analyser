package de.prkz.twitch.emoteanalyser.emote;

import com.github.twitch4j.helix.TwitchHelix;
import com.github.twitch4j.helix.TwitchHelixBuilder;
import com.github.twitch4j.helix.domain.ChannelSearchList;
import com.github.twitch4j.helix.domain.ChannelSearchResult;
import com.netflix.hystrix.exception.HystrixRuntimeException;
import de.prkz.twitch.emoteanalyser.Message;
import de.prkz.twitch.emoteanalyser.emote.provider.*;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class EmoteExtractor extends RichFlatMapFunction<Message, Emote> {

    private static final Logger LOG = LoggerFactory.getLogger(EmoteExtractor.class);

    private transient Instant lastEmoteFetch;
    private transient Set<String> emotes;
    private transient Map<String, Channel> channels;
    private transient TwitchHelix twitch;
    private transient List<EmoteProvider> emoteProviders;

    private final String jdbcUrl;
    private final String emotesTableName;
    private final String channelsTableName;
    private final String twitchClientId;
    private final String twitchClientSecret;
    private final Duration emoteFetchInterval;
    private final Duration emoteFetchTimeout;

    public EmoteExtractor(String jdbcUrl,
                          String emotesTableName,
                          String channelsTableName,
                          String twitchClientId,
                          String twitchClientSecret,
                          Duration emoteFetchInterval,
                          Duration emoteFetchTimeout) {
        this.jdbcUrl = jdbcUrl;
        this.emotesTableName = emotesTableName;
        this.channelsTableName = channelsTableName;
        this.twitchClientId = twitchClientId;
        this.twitchClientSecret = twitchClientSecret;
        this.emoteFetchInterval = emoteFetchInterval;
        this.emoteFetchTimeout = emoteFetchTimeout;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        twitch = TwitchHelixBuilder.builder()
                .withClientId(twitchClientId)
                .withClientSecret(twitchClientSecret)
                .build();

        emoteProviders = Arrays.asList(
                new TwitchEmoteProvider(twitch, emoteFetchTimeout),
                new BTTVEmoteProvider(emoteFetchTimeout),
                new FFZEmoteProvider(emoteFetchTimeout),
                new SevenTVEmoteProvider(emoteFetchTimeout)
        );

        emotes = new HashSet<>();
        channels = new HashMap<>();
        reloadEmotes();
        lastEmoteFetch = Instant.now();
    }

    @Override
    public void flatMap(Message message, Collector<Emote> collector) throws Exception {
        String channelName = message.channel.toLowerCase();
        if (!channels.containsKey(channelName)) {
            channels.put(channelName, new Channel(channelName));

            // Force emote reload
            lastEmoteFetch = Instant.ofEpochMilli(0);
        }

        final var now = Instant.now();
        if (now.isAfter(lastEmoteFetch.plus(emoteFetchInterval))) {
            try {
                reloadEmotes();
            } finally {
                lastEmoteFetch = now;
            }
        }

        String[] words = message.message.split("\\s+");
        for (String word : words) {
            if (emotes.contains(word)) {
                Emote e = new Emote();
                e.instant = message.instant;
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

            // Check for new channel emotes
            synchronizeChannelsWithDatabase(conn);
            for (Channel channel : channels.values()) {
                for (EmoteProvider provider : emoteProviders) {
                    EmoteFetchResult result;
                    try {
                        result = provider.fetchChannelEmotes(channel);
                    } catch (Exception e) {
                        LOG.error("Could not fetch channel emotes for channel '{}' from provider {}",
                                channel.name, provider.getClass().getCanonicalName(), e);
                        continue;
                    }

                    if (result != null) {
                        LOG.info("Fetched {} emotes from provider {} in channel '{}'", result.getEmotes().size(),
                                provider.getClass().getCanonicalName(), channel.name);

                        insertNewEmotes(conn, result.getEmotes(), result.getEmoteType(), result.getChannel());
                    } else {
                        LOG.info("There are no channel emotes for channel '{}' in provider {}",
                                channel.name, provider.getClass().getCanonicalName());
                    }
                }
            }

            // Re-fetch all emotes from table
            emotes.clear();

            try (ResultSet result = stmt.executeQuery("SELECT emote FROM " + emotesTableName)) {
                while (result.next()) {
                    emotes.add(result.getString(1));
                }
            }

            long duration = System.currentTimeMillis() - startTime;

            LOG.info("Updated emote table in {} ms. Now using {} emotes in {} known channels",
                    duration, emotes.size(), channels.size());
        }
    }

    private void synchronizeChannelsWithDatabase(Connection conn) throws SQLException {
        // Fetch existing channels from DB
        try (Statement stmt = conn.createStatement(); ResultSet channelsResult =
                stmt.executeQuery("SELECT channel, broadcaster_id FROM " + channelsTableName)) {
            while (channelsResult.next()) {
                String channelName = channelsResult.getString("channel").toLowerCase();
                String broadcasterId = channelsResult.getString("broadcaster_id");

                Channel channel = channels.get(channelName);
                if (channel == null) {
                    channel = new Channel(channelName);
                    channels.put(channelName, channel);
                }

                if (channel.broadcasterId == null)
                    channel.broadcasterId = broadcasterId;
            }
        }

        // Add newly seen channels to DB and/or fetch missing broadcaster ID
        String sql = "INSERT INTO " + channelsTableName + "(channel, broadcaster_id)\n" +
                "VALUES(?, ?)\n" +
                "ON CONFLICT(channel) DO UPDATE SET broadcaster_id = EXCLUDED.broadcaster_id";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            for (Channel channel : channels.values()) {
                // Newly seen channels also have broadcastId == null
                if (channel.broadcasterId == null) {
                    channel.broadcasterId = fetchTwitchBroadcasterId(channel.name);
                    if (channel.broadcasterId != null) {
                        stmt.setString(1, channel.name);
                        stmt.setString(2, channel.broadcasterId);
                        stmt.executeUpdate();
                    }
                }
            }
        }
    }

    private String fetchTwitchBroadcasterId(String channelName) {
        if (channelName == null)
            throw new IllegalArgumentException("channelName is null");

        ChannelSearchList channelSearchList;
        try {
            channelSearchList = twitch.searchChannels(null, channelName, 1, null, false).execute();
        } catch (HystrixRuntimeException e) {
            LOG.error("Error while trying to fetch broadcaster id for channel '" + channelName + "'", e);
            return null;
        }

        String broadcasterId = null;
        if (!channelSearchList.getResults().isEmpty()) {
            ChannelSearchResult broadcaster = channelSearchList.getResults().get(0);
            if (broadcaster.getBroadcasterLogin().equalsIgnoreCase(channelName))
                broadcasterId = broadcaster.getId();
        }

        if (broadcasterId != null) {
            LOG.info("Found broadcasterId = {} for channel '{}'", broadcasterId, channelName);
            return broadcasterId;
        } else {
            LOG.warn("Channel '{}' has no broadcaster id yet, but we couldn't find it either", channelName);
            return null;
        }
    }

    private void insertNewEmotes(Connection conn, Collection<String> emotes, EmoteType type, Channel channel)
            throws Exception {
        String sql = "INSERT INTO " + emotesTableName + "(emote, type, channel) " +
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
        stmt.execute("CREATE TABLE IF NOT EXISTS " + emotesTableName + "(" +
                "emote VARCHAR NOT NULL," +
                "type SMALLINT NOT NULL DEFAULT 0," +
                "channel VARCHAR," + // null if global
                "PRIMARY KEY(emote))");

        // If table is empty, insert some default emotes, so we have something to track
        try (ResultSet emoteCountResult = stmt.executeQuery("SELECT COUNT(emote) FROM " + emotesTableName)) {
            emoteCountResult.next();
            if (emoteCountResult.getInt(1) == 0) {
                stmt.execute("INSERT INTO emotes(emote, type) VALUES('Kappa', 1), ('PogChamp', 1), ('DansGame', 1);");
            }
        }


        // Channels table
        stmt.execute("CREATE TABLE IF NOT EXISTS " + channelsTableName + "(" +
                "channel VARCHAR NOT NULL PRIMARY KEY," +
                "broadcaster_id VARCHAR," +
                "hidden BOOLEAN NOT NULL DEFAULT false)");
    }
}
