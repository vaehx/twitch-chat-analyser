package de.prkz.twitch.emoteanalyser.bot;

import com.github.twitch4j.helix.TwitchHelix;
import com.github.twitch4j.helix.TwitchHelixBuilder;
import com.github.twitch4j.helix.domain.ChannelSearchList;
import com.github.twitch4j.helix.domain.ChannelSearchResult;
import de.prkz.twitch.emoteanalyser.Dated;
import de.prkz.twitch.emoteanalyser.Message;
import de.prkz.twitch.emoteanalyser.MessageSerializer;
import de.prkz.twitch.emoteanalyser.ThrowingConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.pircbotx.PircBotX;
import org.pircbotx.delay.StaticDelay;
import org.pircbotx.hooks.ListenerAdapter;
import org.pircbotx.hooks.events.ActionEvent;
import org.pircbotx.hooks.events.ConnectAttemptFailedEvent;
import org.pircbotx.hooks.events.MessageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.impl.StaticLoggerBinder;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class Bot extends ListenerAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(Bot.class);

    private static final long STREAM_UPDATE_COOLDOWN_MILLIS = 150 * 1000L;

    private KafkaProducer<Long, Message> producer;
    private TwitchHelix twitch;
    private final Map<String, Dated<ChannelSearchResult>> livestreams = new HashMap<>();
    private Connection dbConn;
    private String twitchClientId;
    private String twitchClientSecret;
    private String jdbcUrl;
    private String streamsTableName;
    private List<String> channels;
    private String kafkaBootstrapServer;
    private String kafkaTopic;


    public static void main(String[] args) throws Exception {
        System.out.println(LOG.getName());

        final StaticLoggerBinder binder = StaticLoggerBinder.getSingleton();
        System.out.println(binder.getLoggerFactory());
        System.out.println(binder.getLoggerFactoryClassStr());

        if (args.length <= 1) {
            System.err.println("Requires arguments: <twitch-client-id> <twitch-client-secret> <jdbc-url> " +
                    "<streams-table-name> <kafka-bootstrap-server> <kafka-topic> <channel...>");
            System.exit(1);
        }

        Bot bot = new Bot();

        int i = 0;
        bot.twitchClientId = args[i++];
        bot.twitchClientSecret = args[i++];
        bot.jdbcUrl = args[i++];
        bot.streamsTableName = args[i++];
        bot.kafkaBootstrapServer = args[i++];
        bot.kafkaTopic = args[i++];

        bot.channels = new ArrayList<>();
        for (; i < args.length; ++i) {
            LOG.info("Will join channel " + args[i]);
            bot.channels.add(args[i]);
        }

        bot.start();
    }

    private void start() throws Exception {
        LOG.info("Connecting to Database...");
        Class.forName(org.postgresql.Driver.class.getCanonicalName());
        dbConn = DriverManager.getConnection(jdbcUrl);

        LOG.info("Setting up streams table '{}', if it doesn't exist yet...", streamsTableName);
        try (Statement stmt = dbConn.createStatement()) {
            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS " + streamsTableName + "(" +
                    "id SERIAL PRIMARY KEY," +
                    "channel VARCHAR NOT NULL," +
                    "channel_id VARCHAR NOT NULL," +
                    "started_at TIMESTAMP WITH TIME ZONE NOT NULL," +
                    "ended_at TIMESTAMP WITH TIME ZONE NOT NULL)");
        }

        LOG.info("Connecting to Twitch API...");
        twitch = TwitchHelixBuilder.builder()
                .withClientId(twitchClientId)
                .withClientSecret(twitchClientSecret)
                .build();

        LOG.info("Connecting to Kafka cluster...");
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", kafkaBootstrapServer);
        kafkaProps.put("key.serializer", LongSerializer.class);
        kafkaProps.put("value.serializer", MessageSerializer.class);
        kafkaProps.put("linger.ms", 100);

        producer = new KafkaProducer<>(kafkaProps);

        // Pircbot needs # in fron of the channel name
        List<String> pircbotChannels = channels.stream().map(ch -> "#" + ch).collect(Collectors.toList());

        org.pircbotx.Configuration config = new org.pircbotx.Configuration.Builder()
                .setName("justinfan92834") // TODO: Make random?
                .addServer("irc.chat.twitch.tv", 6667)
                .addListener(this)
                .addAutoJoinChannels(pircbotChannels)
                .setAutoReconnect(true)
                .setAutoReconnectAttempts(20)
                .setAutoReconnectDelay(new StaticDelay(10000))
                .buildConfiguration();

        LOG.info("Now starting bot...");
        PircBotX pircBotX = new PircBotX(config);
        pircBotX.startBot();
    }

    @Override
    public void onAction(ActionEvent event) throws Exception {
        // Includes BOT messages, we convert them to normal messages

        if (event.getUser() == null || event.getChannel() == null)
            return;

        Message m = new Message();
        m.channel = cleanupChannelName(event.getChannel().getName());
        m.timestamp = event.getTimestamp();
        m.username = event.getUser().getNick();
        m.message = event.getAction();

        producer.send(new ProducerRecord<>(kafkaTopic, m.timestamp, m));
    }

    @Override
    public void onMessage(MessageEvent event) throws Exception {
        if (event.getUser() == null)
            return;

        Message m = new Message();
        m.channel = cleanupChannelName(event.getChannel().getName());
        m.timestamp = event.getTimestamp();
        m.username = event.getUser().getNick();
        m.message = event.getMessage();

        producer.send(new ProducerRecord<>(kafkaTopic, m.timestamp, m));
    }

    @Override
    public void onConnectAttemptFailed(ConnectAttemptFailedEvent event) {
        if (event.getRemainingAttempts() <= 0) {
            int attempts = event.getBot().getConfiguration().getAutoReconnectAttempts();
            LOG.error("Bot could not reconnect after " + attempts + " attempts. " +
                    "Forcing crash to restart service...");

            System.exit(1);
        }
    }


    /**
     * Updates stream info in DB for all channels that the bot is configured to listen to
     */
    private void updateAllStreamsInfo() {
        // Check if *any* channel needs update. This avoid re-opening the prepared statement for every single message
        final long now = System.currentTimeMillis();
        if (livestreams.values().stream().noneMatch(d -> (now - d.updatedAt() > STREAM_UPDATE_COOLDOWN_MILLIS)))
            return;

        final String sql = "INSERT INTO " + streamsTableName + "(channel, channel_id, started_at, ended_at) " +
                "VALUES(?, ?, to_timestamp(? * 0.001) at time zone utc, to_timestamp(? * 0.001) at time zone utc) " +
                "ON CONFLICT DO UPDATE SET ended_at = excluded.ended_at";
        try (final PreparedStatement stmt = dbConn.prepareStatement(sql)) {
            channels.forEach(channel -> {
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
        channel = channel.toLowerCase();

        Dated<ChannelSearchResult> stream = livestreams.get(channel);

        if (stream != null) {
            if (System.currentTimeMillis() - stream.updatedAt() < STREAM_UPDATE_COOLDOWN_MILLIS)
                return; // info still up to date
        }

        // Fetch current info: We're only interested in live channels
        ChannelSearchList searchList = twitch.searchChannels(null, channel, 1, null, true).execute();
        boolean isOffline;
        if (!searchList.getResults().isEmpty()) {
            isOffline = false;
            if (stream == null) {
                // Stream just went live
                stream = new Dated<>();
                livestreams.put(channel, stream);
            }

            stream.set(searchList.getResults().get(0));
        } else {
            isOffline = true;
        }

        if (stream != null) {
            // Update end-time of existing stream info regardless of whether it just went offline or not
            try {
                upsertCallback.apply(stream.get());
                LOG.info("Updated stream info in DB for channel '{}'", channel);
            } catch (Exception e) {
                LOG.error("Could not update stream info for channel '{}' in DB", channel, e);
            }
        }

        if (isOffline)
            livestreams.remove(channel);
    }



    /**
     * Removes potential '#' from start of channel name and returns the cleaned-up version
     */
    private static String cleanupChannelName(String channel) {
        return channel.startsWith("#") ? channel.substring(1) : channel;
    }
}
