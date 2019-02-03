package de.prkz.twitch.emoteanalyser.user;

import de.prkz.twitch.emoteanalyser.AbstractStatsAggregation;
import de.prkz.twitch.emoteanalyser.Message;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class UserStatsAggregation
        extends AbstractStatsAggregation<Message, Tuple2<String, String>, UserStats> {

    private static final String TABLE_NAME = "user_stats";

    public UserStatsAggregation(String jdbcUrl, long aggregationIntervalMillis, long triggerIntervalMillis) {
        super(jdbcUrl, aggregationIntervalMillis, triggerIntervalMillis);
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
                "VALUES(?, ?, ?, ?) " +
                "ON CONFLICT(channel, username, timestamp) DO UPDATE SET " +
                "messages = " + TABLE_NAME + ".messages + EXCLUDED.messages";
    }

    @Override
    protected int[] getUpsertTypes() {
        return new int[] {Types.BIGINT, Types.VARCHAR, Types.VARCHAR, Types.INTEGER};
    }

    @Override
    protected Collection<Row> prepareStatsForOutput(UserStats stats) {
        List<Row> rows = new ArrayList<>();

        Row latest = new Row(4);
        latest.setField(0, stats.timestamp);
        latest.setField(1, stats.channel);
        latest.setField(2, stats.username);
        latest.setField(3, stats.messageCount);
        rows.add(latest);

        Row total = new Row(4);
        total.setField(0, LATEST_TOTAL_TIMESTAMP);
        total.setField(1, stats.channel);
        total.setField(2, stats.username);
        total.setField(3, stats.messageCount);
        rows.add(total);

        return rows;
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
        return message.timestamp;
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
