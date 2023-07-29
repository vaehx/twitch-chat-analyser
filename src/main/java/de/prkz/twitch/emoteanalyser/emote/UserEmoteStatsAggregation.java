package de.prkz.twitch.emoteanalyser.emote;

import de.prkz.twitch.emoteanalyser.AbstractStatsAggregation;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.sql.*;
import java.time.Duration;

// Key: (channel, emote, username)
public class UserEmoteStatsAggregation
        extends AbstractStatsAggregation<Emote, Tuple3<String, String, String>, UserEmoteStats> {

    private static final String TABLE_NAME = "user_emote_stats";

    public UserEmoteStatsAggregation(String jdbcUrl, Duration aggregationInterval, Duration triggerInterval) {
        super(jdbcUrl, aggregationInterval, triggerInterval);
    }

    @Override
    protected UserEmoteStats createNewStatsForKey(Tuple3<String, String, String> key) {
        UserEmoteStats stats = new UserEmoteStats();
        stats.channel = key.f0;
        stats.emote = key.f1;
        stats.username = key.f2;
        return stats;
    }

    @Override
    protected UserEmoteStats aggregate(UserEmoteStats stats, Emote element) {
        stats.occurrences++;
        return stats;
    }

    @Override
    public void prepareTable(Statement stmt) throws SQLException {
        stmt.execute("CREATE TABLE IF NOT EXISTS " + TABLE_NAME + "(" +
                "channel VARCHAR(32) NOT NULL," +
                "emote VARCHAR(" + Emote.MAX_EMOTE_LENGTH + ") NOT NULL," +
                "username VARCHAR(32) NOT NULL," +
                "timestamp BIGINT NOT NULL," +
                "occurrences INT NOT NULL DEFAULT 0," +
                "PRIMARY KEY(channel, emote, username, timestamp))");
    }

    @Override
    protected String getUpsertSql() {
        return "INSERT INTO " + TABLE_NAME + "(timestamp, channel, emote, username, occurrences) " +
                "VALUES (?, ?, ?, ?, ?), (?, ?, ?, ?, ?) " +
                "ON CONFLICT(channel, emote, username, timestamp) DO UPDATE SET " +
                "occurrences = " + TABLE_NAME + ".occurrences + EXCLUDED.occurrences";
    }

    @Override
    protected void setFieldsForOutput(PreparedStatement stmt, UserEmoteStats stats) throws SQLException {
        // diff
        stmt.setLong(1, stats.instant.toEpochMilli());
        stmt.setString(2, stats.channel);
        stmt.setString(3, stats.emote);
        stmt.setString(4, stats.username);
        stmt.setLong(5, stats.occurrences);

        // total
        stmt.setLong(6, LATEST_TOTAL_TIMESTAMP);
        stmt.setString(7, stats.channel);
        stmt.setString(8, stats.emote);
        stmt.setString(9, stats.username);
        stmt.setLong(10, stats.occurrences);
    }

    @Override
    protected TypeInformation<UserEmoteStats> getStatsTypeInfo() {
        return TypeInformation.of(new TypeHint<UserEmoteStats>() {});
    }

    @Override
    protected TypeInformation<Tuple2<Tuple3<String, String, String>, Long>> getKeyTypeInfo() {
        return TypeInformation.of(new TypeHint<Tuple2<Tuple3<String, String, String>, Long>>() {});
    }

    @Override
    protected long getTimestampForElement(Emote emote) {
        return emote.instant.toEpochMilli();
    }

    @Override
    protected Tuple3<String, String, String> getKeyForElement(Emote emote) {
        return new Tuple3<>(emote.channel, emote.emote, emote.username);
    }

    @Override
    protected Integer getHashForElement(Emote emote) {
        return (emote.channel + "," + emote.emote + "," + emote.username).hashCode();
    }
}
