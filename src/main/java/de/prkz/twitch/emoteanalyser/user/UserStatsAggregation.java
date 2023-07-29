package de.prkz.twitch.emoteanalyser.user;

import de.prkz.twitch.emoteanalyser.AbstractStatsAggregation;
import de.prkz.twitch.emoteanalyser.Message;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;

import java.sql.*;
import java.time.Duration;

public class UserStatsAggregation
        extends AbstractStatsAggregation<Message, Tuple2<String, String>, UserStats> {

    private static final String TABLE_NAME = "user_stats";

    public UserStatsAggregation(String jdbcUrl, Duration aggregationInterval, Duration triggerInterval) {
        super(jdbcUrl, aggregationInterval, triggerInterval);
    }

    @Override
    protected UserStats createNewStatsForKey(Tuple2<String, String> key) {
        UserStats stats = new UserStats();
        stats.channel = key.f0;
        stats.username = key.f1;
        return stats;
    }

    @Override
    protected UserStats aggregate(UserStats stats, Message element) {
        stats.messageCount++;
        return stats;
    }

    @Override
    public void prepareTable(Statement stmt) throws SQLException {
        stmt.execute("CREATE TABLE IF NOT EXISTS " + TABLE_NAME + "(" +
                "channel VARCHAR(32) NOT NULL," +
                "username VARCHAR(32) NOT NULL," +
                "timestamp BIGINT NOT NULL," +
                "messages INT NOT NULL DEFAULT 0," +
                "PRIMARY KEY(channel, username, timestamp))");
    }

    @Override
    protected String getUpsertSql() {
        return "INSERT INTO " + TABLE_NAME + "(timestamp, channel, username, messages) " +
                "VALUES (?, ?, ?, ?), (?, ?, ?, ?) " +
                "ON CONFLICT(channel, username, timestamp) DO UPDATE SET " +
                "messages = " + TABLE_NAME + ".messages + EXCLUDED.messages";
    }

    @Override
    protected void setFieldsForOutput(PreparedStatement stmt, UserStats stats) throws SQLException {
        // diff
        stmt.setLong(1, stats.instant.toEpochMilli());
        stmt.setString(2, stats.channel);
        stmt.setString(3, stats.username);
        stmt.setLong(4, stats.messageCount);

        // total
        stmt.setLong(5, LATEST_TOTAL_TIMESTAMP);
        stmt.setString(6, stats.channel);
        stmt.setString(7, stats.username);
        stmt.setLong(8, stats.messageCount);
    }

    @Override
    protected TypeInformation<UserStats> getStatsTypeInfo() {
        return TypeInformation.of(new TypeHint<UserStats>() {});
    }

    @Override
    protected TypeInformation<Tuple2<Tuple2<String, String>, Long>> getKeyTypeInfo() {
        return TypeInformation.of(new TypeHint<Tuple2<Tuple2<String, String>, Long>>() {});
    }

    @Override
    protected long getTimestampForElement(Message message) {
        return message.instant.toEpochMilli();
    }

    @Override
    protected Tuple2<String, String> getKeyForElement(Message message) {
        return new Tuple2<>(message.channel, message.username);
    }

    @Override
    protected Integer getHashForElement(Message message) {
        return (message.channel + "," + message.username).hashCode();
    }
}
