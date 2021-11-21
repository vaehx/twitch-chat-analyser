package de.prkz.twitch.emoteanalyser;

import de.prkz.twitch.emoteanalyser.channel.ChannelStatsAggregation;
import de.prkz.twitch.emoteanalyser.config.FlinkJobConfig;
import de.prkz.twitch.emoteanalyser.emote.*;
import de.prkz.twitch.emoteanalyser.phrase.*;
import de.prkz.twitch.emoteanalyser.user.UserStatsAggregation;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
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


        if (args.length == 0) {
            System.out.println("Arguments: <path/to/config.properties>");
            System.exit(1);
        }

        FlinkJobConfig config = new FlinkJobConfig(Paths.get(args[0]));


        // Prepare database
        Connection conn = DriverManager.getConnection(config.getDbJdbcUrl());
        Statement stmt = conn.createStatement();


        // Create stream environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Synchronous checkpoints to local file system
        env.enableCheckpointing(CHECKPOINT_INTERVAL_MS);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("file:///data/checkpoints");

        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                5, org.apache.flink.api.common.time.Time.minutes(1), Time.seconds(5)));

        // Pull messages from Kafka
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", config.getKafkaBootstrapServers());
        kafkaProps.setProperty("group.id", "twitch_chat_analyser");
        kafkaProps.setProperty("auto.offset.reset", "earliest");

        FlinkKafkaConsumer<Message> consumer =
                new FlinkKafkaConsumer<>(config.getKafkaTopic(), new MessageDeserializationSchema(), kafkaProps);
        DataStream<Message> messages = env
                .addSource(consumer)
                .uid("KafkaSource_0")
                .name("KafkaSource")
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Message>noWatermarks()
                        .withTimestampAssigner((message, recordTs) -> message.timestamp))
                .uid("MessageTimestamps_0");

        // Per-Channel statistics
        ChannelStatsAggregation channelStatsAggregation = new ChannelStatsAggregation(
                config.getDbJdbcUrl(),
                config.getAggregationIntervalMillis(),
                config.getTriggerIntervalMillis());
        channelStatsAggregation.prepareTable(stmt);
        channelStatsAggregation.aggregateAndExportFrom(messages, PARALLELISM, "ChannelStats");

        // Per-User statistics
        UserStatsAggregation userStatsAggregation = new UserStatsAggregation(
                config.getDbJdbcUrl(),
                config.getAggregationIntervalMillis(),
                config.getTriggerIntervalMillis());
        userStatsAggregation.prepareTable(stmt);
        userStatsAggregation.aggregateAndExportFrom(messages, PARALLELISM, "UserStats");


        // Extract emotes from messages
        EmoteExtractor emoteExtractor = new EmoteExtractor(
                config.getDbJdbcUrl(),
                config.getDbEmotesTableName(),
                config.getDbChannelsTableName(),
                config.getTwitchClientId(),
                config.getTwitchClientSecret(),
                config.getEmoteFetchIntervalMillis(),
                config.getEmoteFetchTimeoutMillis());
        emoteExtractor.prepareTables(stmt);
        DataStream<Emote> emotes = messages
                .flatMap(emoteExtractor)
                .name("ExtractEmotes")
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Emote>noWatermarks()
                        .withTimestampAssigner((emote, recordTs) -> emote.timestamp))
                .uid("EmoteTimestamps_0");

        // Per-Emote statistics
        EmoteStatsAggregation emoteStatsAggregation = new EmoteStatsAggregation(
                config.getDbJdbcUrl(),
                config.getAggregationIntervalMillis(),
                config.getTriggerIntervalMillis());
        emoteStatsAggregation.prepareTable(stmt);
        emoteStatsAggregation.aggregateAndExportFrom(emotes, PARALLELISM, "EmoteStats");

        // Per-Emote per-User statistics
        UserEmoteStatsAggregation userEmoteStatsAggregation = new UserEmoteStatsAggregation(
                config.getDbJdbcUrl(),
                config.getAggregationIntervalMillis(),
                config.getTriggerIntervalMillis());
        userEmoteStatsAggregation.prepareTable(stmt);
        userEmoteStatsAggregation.aggregateAndExportFrom(emotes, PARALLELISM, "UserEmoteStats");


        // Phrase (regex) statistics
        PhraseExtractor.prepareTables(stmt);
        PhraseExtractor phraseExtractor = new PhraseExtractor(config.getDbJdbcUrl());

        SingleOutputStreamOperator<PhraseStats> matchedPhrases = messages
                .process(phraseExtractor)
                .name("PhraseExtractor");

        PhraseStatsAggregation phraseStatsAggregation = new PhraseStatsAggregation(
                config.getDbJdbcUrl(),
                config.getAggregationIntervalMillis(),
                config.getTriggerIntervalMillis());
        phraseStatsAggregation.prepareTable(stmt);
        phraseStatsAggregation.aggregateAndExportFrom(matchedPhrases, PARALLELISM, "PhraseStats");

        MessagesMatchingPhraseExporter.prepareTables(stmt);
        matchedPhrases
                .getSideOutput(phraseExtractor.getMatchedMessagesOutputTag())
                .addSink(new MessagesMatchingPhraseExporter(config.getDbJdbcUrl()))
                .name("MessagesMatchingPhraseExporter");


        stmt.close();
        conn.close();

        env.execute("EmoteAnalysis");
    }
}
