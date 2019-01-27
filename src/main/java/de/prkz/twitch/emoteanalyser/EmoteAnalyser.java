package de.prkz.twitch.emoteanalyser;

import de.prkz.twitch.emoteanalyser.channel.ChannelStatsAggregation;
import de.prkz.twitch.emoteanalyser.emote.*;
import de.prkz.twitch.emoteanalyser.user.UserStatsAggregation;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;

public class EmoteAnalyser {

    private static final Logger LOG = LoggerFactory.getLogger(EmoteAnalyser.class);

    private static final long CHECKPOINT_INTERVAL_MS = 60000;

    public static void main(String[] args) throws Exception {

        LOG.debug("test-debug");
        LOG.info("test-info");
        LOG.warn("test-warn");


        // Parse arguments
        if (args.length < 6) {
            System.out.println("Arguments: <jdbcUrl> <kafka-bootstrap-server> <aggregation-interval-ms> " +
                    "<trigger-interval-ms> <max-out-of-orderness-ms> <db-batch-size>");
            System.exit(1);
        }

        String jdbcUrl = args[0];
        String kafkaBootstrapServer = args[1];
        long aggregationIntervalMs = Long.parseLong(args[2]);
        long triggerIntervalMs = Long.parseLong(args[3]);
        long maxOutOfOrdernessMs = Long.parseLong(args[4]);
        int dbBatchSize = Integer.parseInt(args[5]);

        // Prepare database
        Connection conn = DriverManager.getConnection(jdbcUrl);
        Statement stmt = conn.createStatement();


        // Create stream environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Synchronous checkpoints to local file system
        env.enableCheckpointing(CHECKPOINT_INTERVAL_MS);
        env.setStateBackend((StateBackend) new FsStateBackend("file:///data/checkpoints", false));

        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                5, org.apache.flink.api.common.time.Time.minutes(1), Time.seconds(5)));

        // Pull messages from Kafka
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", kafkaBootstrapServer);
        kafkaProps.setProperty("group.id", "twitch_chat_analyzer");
        kafkaProps.setProperty("auto.offset.reset", "earliest");

        FlinkKafkaConsumer<Message> consumer =
                new FlinkKafkaConsumer<>("TwitchMessages", new MessageDeserializationSchema(), kafkaProps);
        DataStream<Message> messages = env
                .addSource(consumer)
                .uid("KafkaSource_0")
                .name("KafkaSource")
                .assignTimestampsAndWatermarks(new Message.TimestampExtractor(maxOutOfOrdernessMs))
                .uid("MessageTimestamps_0");

        // There are typically much less rows written for channels
        int channelStatsBatchSize = (int) Math.max(1, Math.round(dbBatchSize * 0.2));

        // Per-Channel statistics
        ChannelStatsAggregation channelStatsAggregation =
                new ChannelStatsAggregation(jdbcUrl, channelStatsBatchSize, aggregationIntervalMs, triggerIntervalMs);
        channelStatsAggregation.prepareTable(stmt);
        channelStatsAggregation.aggregateAndExportFrom(messages, "ChannelStats");

        // Per-User statistics
        UserStatsAggregation userStatsAggregation =
                new UserStatsAggregation(jdbcUrl, dbBatchSize, aggregationIntervalMs, triggerIntervalMs);
        userStatsAggregation.prepareTable(stmt);
        userStatsAggregation.aggregateAndExportFrom(messages, "UserStats");


        // Extract emotes from messages
        EmoteExtractor emoteExtractor = new EmoteExtractor(jdbcUrl);
        emoteExtractor.prepareTables(stmt);
        DataStream<Emote> emotes = messages
                .flatMap(emoteExtractor)
                .name("ExtractEmotes")
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Emote>() {
                    @Override
                    public long extractAscendingTimestamp(Emote emote) {
                        return emote.timestamp;
                    }
                })
                .uid("EmoteTimestamps_0");

        // Per-Emote statistics
        EmoteStatsAggregation emoteStatsAggregation =
                new EmoteStatsAggregation(jdbcUrl, dbBatchSize, aggregationIntervalMs, triggerIntervalMs);
        emoteStatsAggregation.prepareTable(stmt);
        emoteStatsAggregation.aggregateAndExportFrom(emotes, "EmoteStats");

        // Per-Emote per-User statistics
        UserEmoteStatsAggregation userEmoteStatsAggregation =
                new UserEmoteStatsAggregation(jdbcUrl, dbBatchSize, aggregationIntervalMs, triggerIntervalMs);
        userEmoteStatsAggregation.prepareTable(stmt);
        userEmoteStatsAggregation.aggregateAndExportFrom(emotes, "UserEmoteStats");


        stmt.close();
        conn.close();

        env.execute("EmoteAnalysis");
    }
}
