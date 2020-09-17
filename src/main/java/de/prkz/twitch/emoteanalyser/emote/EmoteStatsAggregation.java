package de.prkz.twitch.emoteanalyser.emote;

import de.prkz.twitch.emoteanalyser.AbstractStatsAggregation;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;

import java.sql.*;

// Key: (channel, emote)
public class EmoteStatsAggregation
        extends AbstractStatsAggregation<Emote, Tuple2<String, String>, EmoteStats> {

    private static final String TABLE_NAME = "emote_stats";

    public EmoteStatsAggregation(String jdbcUrl, long aggregationIntervalMillis, long triggerIntervalMillis) {
        super(jdbcUrl, aggregationIntervalMillis, triggerIntervalMillis);
    }

    @Override
    protected EmoteStats createNewStatsForKey(Tuple2<String, String> key) {
        EmoteStats stats = new EmoteStats();
        stats.channel = key.f0;
        stats.emote = key.f1;
        return stats;
    }

    @Override
    protected EmoteStats aggregate(EmoteStats stats, Emote element) {
        stats.occurrences++;
        return stats;
    }

    @Override
    public void prepareTable(Statement stmt) throws SQLException {
        stmt.execute("CREATE TABLE IF NOT EXISTS " + TABLE_NAME + "(" +
                "channel VARCHAR(32) NOT NULL," +
                "emote VARCHAR(64) NOT NULL," +
                "timestamp BIGINT NOT NULL," +
                "occurrences BIGINT NOT NULL DEFAULT 0," +
                "PRIMARY KEY(channel, emote, timestamp))");
    }

    @Override
    protected String getUpsertSql() {
        return "INSERT INTO " + TABLE_NAME + "(timestamp, channel, emote, occurrences) " +
                "VALUES (?, ?, ?, ?), (?, ?, ?, ?) " +
                "ON CONFLICT(channel, emote, timestamp) DO UPDATE SET " +
                "occurrences = " + TABLE_NAME + ".occurrences + EXCLUDED.occurrences";
    }

    @Override
    protected void setFieldsForOutput(PreparedStatement stmt, EmoteStats stats) throws SQLException {
        // diff
        stmt.setLong(1, stats.timestamp);
        stmt.setString(2, stats.channel);
        stmt.setString(3, stats.emote);
        stmt.setLong(4, stats.occurrences);

        // total
        stmt.setLong(5, LATEST_TOTAL_TIMESTAMP);
        stmt.setString(6, stats.channel);
        stmt.setString(7, stats.emote);
        stmt.setLong(8, stats.occurrences);
    }

    @Override
    protected TypeInformation<EmoteStats> getStatsTypeInfo() {
        return TypeInformation.of(new TypeHint<EmoteStats>() {});
    }

    @Override
    protected TypeInformation<Tuple2<Tuple2<String, String>, Long>> getKeyTypeInfo() {
        return TypeInformation.of(new TypeHint<Tuple2<Tuple2<String, String>, Long>>() {});
    }

    @Override
    protected long getTimestampForElement(Emote emote) {
        return emote.timestamp;
    }

    @Override
    protected Tuple2<String, String> getKeyForElement(Emote emote) {
        return new Tuple2<>(emote.channel, emote.emote);
    }

    @Override
    protected Integer getHashForElement(Emote emote) {
        return (emote.channel + "," + emote.emote).hashCode();
    }
}
