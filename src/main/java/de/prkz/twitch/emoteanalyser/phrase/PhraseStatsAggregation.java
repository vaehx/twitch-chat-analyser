package de.prkz.twitch.emoteanalyser.phrase;

import de.prkz.twitch.emoteanalyser.AbstractStatsAggregation;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;

// Key: (channel, phraseName)
public class PhraseStatsAggregation extends AbstractStatsAggregation<PhraseStats, Tuple2<String, String>, PhraseStats> {

    private static final String TABLE_NAME = "phrase_stats";

    public PhraseStatsAggregation(String jdbcUrl, Duration aggregationInterval, Duration triggerInterval) {
        super(jdbcUrl, aggregationInterval, triggerInterval);
    }

    @Override
    protected PhraseStats createNewStatsForKey(Tuple2<String, String> key) {
        PhraseStats stats = new PhraseStats();
        stats.channel = key.f0;
        stats.phraseName = key.f1;
        return stats;
    }

    @Override
    protected PhraseStats aggregate(PhraseStats stats, PhraseStats element) {
        stats.matches += element.matches;
        return stats;
    }

    @Override
    public void prepareTable(Statement stmt) throws SQLException {
        stmt.execute("CREATE TABLE IF NOT EXISTS " + TABLE_NAME + "(" +
                "channel VARCHAR(32) NOT NULL," +
                "phrase_name VARCHAR(64) NOT NULL," +
                "timestamp BIGINT NOT NULL," +
                "matches BIGINT NOT NULL DEFAULT 0," +
                "PRIMARY KEY(channel, phrase_name, timestamp))");
    }

    @Override
    protected String getUpsertSql() {
        return "INSERT INTO " + TABLE_NAME + "(channel, phrase_name, timestamp, matches) " +
                "VALUES (?, ?, ?, ?), (?, ?, ?, ?) " +
                "ON CONFLICT(channel, phrase_name, timestamp) DO UPDATE SET " +
                "matches = " + TABLE_NAME + ".matches + EXCLUDED.matches";
    }

    @Override
    protected void setFieldsForOutput(PreparedStatement stmt, PhraseStats stats) throws SQLException {
        // diff
        stmt.setString(1, stats.channel);
        stmt.setString(2, stats.phraseName);
        stmt.setLong(3, stats.instant.toEpochMilli());
        stmt.setLong(4, stats.matches);

        // total
        stmt.setString(5, stats.channel);
        stmt.setString(6, stats.phraseName);
        stmt.setLong(7, LATEST_TOTAL_TIMESTAMP);
        stmt.setLong(8, stats.matches);
    }

    @Override
    protected TypeInformation<PhraseStats> getStatsTypeInfo() {
        return TypeInformation.of(new TypeHint<PhraseStats>() {});
    }

    @Override
    protected TypeInformation<Tuple2<Tuple2<String, String>, Long>> getKeyTypeInfo() {
        return TypeInformation.of(new TypeHint<Tuple2<Tuple2<String, String>, Long>>() {});
    }

    @Override
    protected long getTimestampForElement(PhraseStats element) {
        return element.instant.toEpochMilli();
    }

    @Override
    protected Tuple2<String, String> getKeyForElement(PhraseStats element) {
        return new Tuple2<>(element.channel, element.phraseName);
    }

    @Override
    protected Integer getHashForElement(PhraseStats element) {
        return (element.channel + "," + element.phraseName).hashCode();
    }
}
