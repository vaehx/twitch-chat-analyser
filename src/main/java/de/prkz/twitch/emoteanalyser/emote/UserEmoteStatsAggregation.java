package de.prkz.twitch.emoteanalyser.emote;

import de.prkz.twitch.emoteanalyser.AbstractStatsAggregation;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.Row;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

// Key: (channel, emote, username)
public class UserEmoteStatsAggregation
        extends AbstractStatsAggregation<Emote, Tuple3<String, String, String>, UserEmoteStats> {

    private static final String TABLE_NAME = "user_emote_stats";

    public UserEmoteStatsAggregation(String jdbcUrl, long aggregationIntervalMillis, long triggerIntervalMillis) {
        super(jdbcUrl, aggregationIntervalMillis, triggerIntervalMillis);
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
                "emote VARCHAR(64) NOT NULL," +
                "username VARCHAR(32) NOT NULL," +
                "timestamp BIGINT NOT NULL," +
                "total_occurrences INT NOT NULL DEFAULT 0," +
                "occurrences INT NOT NULL DEFAULT 0," +
                "PRIMARY KEY(channel, emote, username, timestamp))");
    }

    @Override
    protected String getUpsertSql() {
        return "INSERT INTO " + TABLE_NAME + "(timestamp, channel, emote, username, total_occurrences, occurrences) " +
                "VALUES(?, ?, ?, ?, ?, ?) " +
                "ON CONFLICT(channel, emote, username, timestamp) DO UPDATE SET " +
                "total_occurrences = " + TABLE_NAME + ".total_occurrences + EXCLUDED.occurrences, " +
                "occurrences = " + TABLE_NAME + ".occurrences + EXCLUDED.occurrences";
    }

    @Override
    protected int[] getUpsertTypes() {
        return new int[] {Types.BIGINT, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.INTEGER, Types.INTEGER};
    }

    @Override
    protected Collection<Row> prepareStatsForOutput(UserEmoteStats stats) {
        List<Row> rows = new ArrayList<>();

        Row latest = new Row(6);
        latest.setField(0, stats.timestamp);
        latest.setField(1, stats.channel);
        latest.setField(2, stats.emote);
        latest.setField(3, stats.username);
        latest.setField(4, stats.occurrences);
        latest.setField(5, stats.occurrences);
        rows.add(latest);

        Row total = new Row(6);
        total.setField(0, LATEST_TOTAL_TIMESTAMP);
        total.setField(1, stats.channel);
        total.setField(2, stats.emote);
        total.setField(3, stats.username);
        total.setField(4, stats.occurrences);
        total.setField(5, stats.occurrences);
        rows.add(total);

        return rows;
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
        return emote.timestamp;
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
