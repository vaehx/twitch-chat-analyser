package de.prkz.twitch.emoteanalyser.user;

import de.prkz.twitch.emoteanalyser.AbstractStatsAggregation;
import de.prkz.twitch.emoteanalyser.Message;
import de.prkz.twitch.emoteanalyser.output.OutputStatement;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

import java.sql.*;

public class UserStatsAggregation
        extends AbstractStatsAggregation<Message, Tuple2<String, String>, UserStats> {

    private static final String TABLE_NAME = "user_stats";

    public UserStatsAggregation(String jdbcUrl,
                                int dbBatchInterval,
                                long aggregationIntervalMillis,
                                long triggerIntervalMillis) {
        super(jdbcUrl, dbBatchInterval, aggregationIntervalMillis, triggerIntervalMillis);
    }

    @Override
    protected TypeInformation<UserStats> getStatsTypeInfo() {
        return TypeInformation.of(new TypeHint<UserStats>() {
        });
    }

    @Override
    protected KeySelector<Message, Tuple2<String, String>> createKeySelector() {
        return new KeySelector<Message, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> getKey(Message message) throws Exception {
                return new Tuple2<>(message.channel, message.username);
            }
        };
    }

    @Override
    protected UserStats createNewStatsForKey(Tuple2<String, String> key) throws SQLException {
        UserStats stats = new UserStats();
        stats.channel = key.f0;
        stats.username = key.f1;

        // Load current count from database, if it exists
        Statement stmt = conn.createStatement();
        ResultSet result;

        result = stmt.executeQuery("SELECT EXISTS(SELECT 1 FROM " + TABLE_NAME + " " +
                "WHERE channel='" + stats.channel + "' AND username='" + stats.username + "')");
        result.next();
        if (result.getBoolean(1)) {
            result = stmt.executeQuery("SELECT total_messages, messages, timestamp FROM " + TABLE_NAME + " " +
                    "WHERE channel='" + stats.channel + "' AND username='" + stats.username + "' " +
                    "ORDER BY timestamp DESC LIMIT 1");
            result.next();
            stats.totalMessageCount = result.getLong(1);
            stats.messageCount = result.getInt(2);
            stats.timestamp = result.getLong(3);
        } else {
            stats.totalMessageCount = 0;
            stats.messageCount = 0;
            stats.timestamp = 0;
        }

        stmt.close();
        return stats;
    }

    @Override
    protected void processWindowElements(UserStats stats, Iterable<Message> messages) {
        stats.messageCount = 0;
        for (Message message : messages) {
            stats.messageCount++;
            stats.totalMessageCount++;
        }
    }

    @Override
    public void prepareTable(Statement stmt) throws SQLException {
        stmt.execute("CREATE TABLE IF NOT EXISTS " + TABLE_NAME + "(" +
                "channel VARCHAR(32) NOT NULL," +
                "username VARCHAR(32) NOT NULL," +
                "timestamp BIGINT NOT NULL," +
                "total_messages INT NOT NULL DEFAULT 0," +
                "messages INT NOT NULL DEFAULT 0," +
                "PRIMARY KEY(channel, username, timestamp))");
    }

    @Override
    protected Iterable<OutputStatement> prepareStatsForOutput(UserStats stats) {
        return OutputStatement.buildBatch()
                .add("INSERT INTO " + TABLE_NAME + "(timestamp, channel, username, total_messages, messages) " +
                        "VALUES(" + stats.timestamp + ", '" + stats.channel + "', '" + stats.username + "', " + stats.totalMessageCount + ", " + stats.messageCount + ") " +
                        "ON CONFLICT(channel, username, timestamp) DO UPDATE " +
                        "SET total_messages = excluded.total_messages, messages = excluded.messages")
                .add("INSERT INTO " + TABLE_NAME + "(timestamp, channel, username, total_messages, messages) " +
                        "VALUES(0, '" + stats.channel + "', '" + stats.username + "', " + stats.totalMessageCount + ", " + stats.messageCount + ") " +
                        "ON CONFLICT(channel, username, timestamp) DO UPDATE " +
                        "SET total_messages = excluded.total_messages, messages = excluded.messages")
                .finish();
    }
}
