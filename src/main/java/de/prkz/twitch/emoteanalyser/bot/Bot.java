package de.prkz.twitch.emoteanalyser.bot;

import com.github.twitch4j.TwitchClient;
import com.github.twitch4j.TwitchClientBuilder;
import com.github.twitch4j.chat.TwitchChat;
import com.github.twitch4j.chat.events.channel.ChannelMessageEvent;
import com.github.twitch4j.common.events.domain.EventChannel;
import com.github.twitch4j.common.events.domain.EventUser;
import com.github.twitch4j.helix.TwitchHelix;
import com.github.twitch4j.helix.domain.ChannelSearchList;
import com.github.twitch4j.helix.domain.ChannelSearchResult;
import de.prkz.twitch.emoteanalyser.Dated;
import de.prkz.twitch.emoteanalyser.Message;
import de.prkz.twitch.emoteanalyser.MessageSerializer;
import de.prkz.twitch.emoteanalyser.ThrowingConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.impl.StaticLoggerBinder;

import java.nio.file.Paths;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * The chat bot collects twitch messages from the configured channels and pushes them to Kafka. Additionally, it
 * tracks uptime of livestreams.
 */
public class Bot {

    private static final Logger LOG = LoggerFactory.getLogger(Bot.class);

    private final BotConfig config;
    private KafkaProducer<Long, Message> producer;
    private TwitchClient twitch;
    private TwitchChat chat;
    private TwitchHelix helix;
    private final Map<String, Dated<ChannelSearchResult>> livestreams = new HashMap<>();
    private Connection dbConn;


    public static void main(String[] args) throws Exception {
        System.out.println(LOG.getName());

        final StaticLoggerBinder binder = StaticLoggerBinder.getSingleton();
        System.out.println(binder.getLoggerFactory());
        System.out.println(binder.getLoggerFactoryClassStr());

        if (args.length == 0) {
            System.err.println("Requires arguments: <path/to/config.properties>");
            System.exit(1);
        }

        final BotConfig config = new BotConfig(Paths.get(args[0]));
        final Bot bot = new Bot(config);
        bot.start();
    }


    public Bot(BotConfig config) {
        this.config = config;
    }

    private void start() throws Exception {
        LOG.info("Connecting to Database...");
        Class.forName(org.postgresql.Driver.class.getCanonicalName());
        dbConn = DriverManager.getConnection(config.getDbJdbcUrl());

        LOG.info("Setting up streams table '{}', if it doesn't exist yet...", config.getStreamsTableName());
        try (Statement stmt = dbConn.createStatement()) {
            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS " + config.getStreamsTableName() + "(" +
                    "channel VARCHAR NOT NULL," +
                    "channel_id VARCHAR NOT NULL," +
                    "started_at TIMESTAMP WITH TIME ZONE NOT NULL," +
                    "ended_at TIMESTAMP WITH TIME ZONE NOT NULL," +
                    "PRIMARY KEY(channel, started_at))");
        }

        LOG.info("Connecting to Kafka cluster...");
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", config.getKafkaBootstrapServers());
        kafkaProps.put("key.serializer", LongSerializer.class);
        kafkaProps.put("value.serializer", MessageSerializer.class);
        kafkaProps.put("linger.ms", 100);
        producer = new KafkaProducer<>(kafkaProps);

        LOG.info("Starting twitch client(s)...");
        twitch = TwitchClientBuilder.builder()
                .withClientId(config.getTwitchClientId())
                .withClientSecret(config.getTwitchClientSecret())
                .withEnableHelix(true)
                .withEnableChat(true)
                .build();

        helix = twitch.getHelix();
        chat = twitch.getChat();

        LOG.info("Updating streams info now...");
        updateAllStreamsInfo();

        for (String channel : config.getChannels()) {
            LOG.info("Will join channel '{}'", channel);
            twitch.getChat().joinChannel(channel);
        }

        chat.getEventManager().onEvent(ChannelMessageEvent.class, this::onMessage);
    }

    private void onMessage(ChannelMessageEvent event) {
        EventUser user = event.getUser();
        if (user == null)
            return;

        EventChannel channel = event.getChannel();
        if (channel == null)
            return;

        String messageText = event.getMessage();
        if (messageText == null)
            return;

        messageText = messageText.trim();
        if (messageText.isEmpty())
            return;

        Message m = new Message();
        m.channel = channel.getName();
        m.instant = event.getFiredAtInstant();
        m.username = user.getName();
        m.message = messageText;

        producer.send(new ProducerRecord<>(config.getKafkaTopic(), m.instant.toEpochMilli(), m));

        updateAllStreamsInfo();
    }


    /**
     * Updates stream info in DB for all channels that the bot is configured to listen to
     */
    private synchronized void updateAllStreamsInfo() {
        // Check if *any* channel needs update. This avoid re-opening the prepared statement for every single message
        final long now = System.currentTimeMillis();
        boolean noStreamNeedsUpdate = livestreams.values().stream()
                .noneMatch(d -> (now - d.updatedAt() >= config.getStreamsUpdateCooldownMillis()));
        if (livestreams.size() == config.getChannels().size() && noStreamNeedsUpdate) {
            return;
        }

        LOG.info("Updating streams info...");

        final String sql = "INSERT INTO " + config.getStreamsTableName() +
                "(channel, channel_id, started_at, ended_at) " +
                "VALUES(?, ?, to_timestamp(? * 0.001) at time zone 'utc', to_timestamp(? * 0.001) at time zone 'utc') " +
                "ON CONFLICT(channel, started_at) DO UPDATE SET ended_at = excluded.ended_at";
        try (final PreparedStatement stmt = dbConn.prepareStatement(sql)) {
            config.getChannels().forEach(channel -> {
                updateStreamInfo(channel, stream -> {
                    stmt.setString(1, channel);
                    stmt.setString(2, stream.getId());
                    stmt.setLong(3, stream.getStartedAt().toEpochMilli());
                    stmt.setLong(4, System.currentTimeMillis());
                    stmt.executeUpdate();
                });
            });
        } catch (SQLException e) {
            LOG.error("Could not prepare statement for sql: {}", sql, e);
        }
    }

    /**
     * Updates current stream info in the database for the given channel. The channel is determined through a
     * channel search query by-name.
     *
     * @param channel case insensitive
     */
    private void updateStreamInfo(String channel, ThrowingConsumer<ChannelSearchResult> upsertCallback) {
        Dated<ChannelSearchResult> stream = livestreams.get(channel);
        if (stream != null) {
            long now = System.currentTimeMillis();
            if (now - stream.updatedAt() < config.getStreamsUpdateCooldownMillis()) {
                LOG.info("Stream info on channel '{}' still up to date (updated {} ms ago)",
                        channel, now - stream.updatedAt());
                return; // info still up to date
            }
        } else {
            stream = new Dated<>();
            livestreams.put(channel, stream);
        }

        // Fetch current info: We're only interested in live channels
        ChannelSearchList searchList = helix.searchChannels(null, channel, 1, null, true).execute();
        boolean isLive = false;
        if (!searchList.getResults().isEmpty()) {
            ChannelSearchResult r = searchList.getResults().get(0);

            // We also have to check for exact match
            if (r.getBroadcasterLogin().equalsIgnoreCase(channel)) {
                isLive = true;
                stream.set(r);
            }
        }

        if (stream.get() != null) { // stream was live before or just went live
            ChannelSearchResult r = stream.get();


            LOG.info("  Channel search result for '{}': isLive={}, startedAt={}",
                    channel, r.getIsLive(), r.getStartedAt());



            if (r.getIsLive() && r.getStartedAt() != null) {
                // Update end-time of existing stream info regardless of whether it just went offline or not
                try {
                    upsertCallback.apply(r);
                    LOG.info("Updated stream info in DB for channel '{}'", channel);
                } catch (Exception e) {
                    LOG.error("Could not update stream info for channel '{}' in DB", channel, e);
                }
            }
        }

        if (!isLive) {
            stream.set(null);
            LOG.info("Channel '{}' is not live.", channel);
        }
    }
}
