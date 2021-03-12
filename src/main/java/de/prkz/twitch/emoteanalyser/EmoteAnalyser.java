package de.prkz.twitch.emoteanalyser;

import de.prkz.twitch.emoteanalyser.channel.ChannelStatsAggregation;
import de.prkz.twitch.emoteanalyser.emote.*;
import de.prkz.twitch.emoteanalyser.phrase.*;
import de.prkz.twitch.emoteanalyser.user.UserStatsAggregation;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Properties;

public class EmoteAnalyser {

    private static final Logger LOG = LoggerFactory.getLogger(EmoteAnalyser.class);

    private static final long CHECKPOINT_INTERVAL_MS = 60000;
    private static final int PARALLELISM = 1;

    public static void main(String[] args) throws Exception {

        LOG.debug("test-debug");
        LOG.info("test-info");
        LOG.warn("test-warn");


        // Parse arguments
        if (args.length < 5) {
            System.out.println("Arguments: <jdbcUrl> <kafka-bootstrap-server> <kafka-topic> <aggregation-interval-ms> " +
                    "<trigger-interval-ms>");
            System.exit(1);
        }

        String jdbcUrl = args[0];
        String kafkaBootstrapServer = args[1];
        String kafkaTopic = args[2];
        long aggregationIntervalMs = Long.parseLong(args[3]);
        long triggerIntervalMs = Long.parseLong(args[4]);

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
        kafkaProps.setProperty("group.id", "twitch_chat_analyser");
        kafkaProps.setProperty("auto.offset.reset", "earliest");

        FlinkKafkaConsumer<Message> consumer =
                new FlinkKafkaConsumer<>(kafkaTopic, new MessageDeserializationSchema(), kafkaProps);
        DataStream<Message> messages = env
                .addSource(consumer)
                .uid("KafkaSource_0")
                .name("KafkaSource")
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Message>noWatermarks()
                        .withTimestampAssigner((message, recordTs) -> message.timestamp))
                .uid("MessageTimestamps_0");

        // Per-Channel statistics
        ChannelStatsAggregation channelStatsAggregation =
                new ChannelStatsAggregation(jdbcUrl, aggregationIntervalMs, triggerIntervalMs);
        channelStatsAggregation.prepareTable(stmt);
        channelStatsAggregation.aggregateAndExportFrom(messages, PARALLELISM, "ChannelStats");

        // Per-User statistics
        UserStatsAggregation userStatsAggregation =
                new UserStatsAggregation(jdbcUrl, aggregationIntervalMs, triggerIntervalMs);
        userStatsAggregation.prepareTable(stmt);
        userStatsAggregation.aggregateAndExportFrom(messages, PARALLELISM, "UserStats");


        // Extract emotes from messages
        EmoteExtractor emoteExtractor = new EmoteExtractor(jdbcUrl);
        emoteExtractor.prepareTables(stmt);
        DataStream<Emote> emotes = messages
                .flatMap(emoteExtractor)
                .name("ExtractEmotes")
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Emote>noWatermarks()
                        .withTimestampAssigner((emote, recordTs) -> emote.timestamp))
                .uid("EmoteTimestamps_0");

        // Per-Emote statistics
        EmoteStatsAggregation emoteStatsAggregation =
                new EmoteStatsAggregation(jdbcUrl, aggregationIntervalMs, triggerIntervalMs);
        emoteStatsAggregation.prepareTable(stmt);
        emoteStatsAggregation.aggregateAndExportFrom(emotes, PARALLELISM, "EmoteStats");

        // Per-Emote per-User statistics
        UserEmoteStatsAggregation userEmoteStatsAggregation =
                new UserEmoteStatsAggregation(jdbcUrl, aggregationIntervalMs, triggerIntervalMs);
        userEmoteStatsAggregation.prepareTable(stmt);
        userEmoteStatsAggregation.aggregateAndExportFrom(emotes, PARALLELISM, "UserEmoteStats");


        // Phrase (regex) statistics
        PhraseExtractor.prepareTables(stmt);
        PhraseExtractor phraseExtractor = new PhraseExtractor(jdbcUrl);

        SingleOutputStreamOperator<PhraseStats> matchedPhrases = messages
                .process(phraseExtractor)
                .name("PhraseExtractor");

        PhraseStatsAggregation phraseStatsAggregation =
                new PhraseStatsAggregation(jdbcUrl, aggregationIntervalMs, triggerIntervalMs);
        phraseStatsAggregation.prepareTable(stmt);
        phraseStatsAggregation.aggregateAndExportFrom(matchedPhrases, PARALLELISM, "PhraseStats");

        MessagesMatchingPhraseExporter.prepareTables(stmt);
        matchedPhrases
                .getSideOutput(phraseExtractor.getMatchedMessagesOutputTag())
                .addSink(new MessagesMatchingPhraseExporter(jdbcUrl))
                .name("MessagesMatchingPhraseExporter");


        stmt.close();
        conn.close();

        env.execute("EmoteAnalysis");
    }
}
