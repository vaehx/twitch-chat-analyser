package de.prkz.twitch.emoteanalyser.emote;

import de.prkz.twitch.emoteanalyser.AbstractStatsAggregation;
import de.prkz.twitch.emoteanalyser.output.OutputStatement;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

import java.sql.*;

// Key: (channel, emote)
public class EmoteStatsAggregation
        extends AbstractStatsAggregation<Emote, Tuple2<String, String>, EmoteStats> {

    private static final String TABLE_NAME = "emote_stats";

    public EmoteStatsAggregation(String jdbcUrl,
                                 int dbBatchInterval,
                                 long aggregationIntervalMillis,
                                 long triggerIntervalMillis) {
        super(jdbcUrl, dbBatchInterval, aggregationIntervalMillis, triggerIntervalMillis);
    }

    @Override
    protected TypeInformation<EmoteStats> getStatsTypeInfo() {
        return TypeInformation.of(new TypeHint<EmoteStats>() {
        });
    }

    @Override
    protected KeySelector<Emote, Tuple2<String, String>> createKeySelector() {
        return new KeySelector<Emote, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> getKey(Emote emote) throws Exception {
                return new Tuple2<>(emote.channel, emote.emote);
            }
        };
    }

    @Override
    protected EmoteStats createNewStatsForKey(Tuple2<String, String> key) throws SQLException {
        EmoteStats stats = new EmoteStats();
        stats.channel = key.f0;
        stats.emote = key.f1;

        // Load current count from database, if it exists
        Statement stmt = conn.createStatement();
        ResultSet result = stmt.executeQuery("SELECT total_occurrences, occurrences, timestamp FROM " + TABLE_NAME + " " +
                "WHERE channel='" + stats.channel + "' AND emote='" + escapeSingleQuotes(stats.emote) + "' " +
                "ORDER BY timestamp DESC LIMIT 1");

        if (result.next()) {
            stats.totalOccurrences = result.getLong(1);
            stats.occurrences = result.getInt(2);
            stats.timestamp = result.getLong(3);
        } else {
            stats.totalOccurrences = 0;
            stats.occurrences = 0;
            stats.timestamp = 0;
        }

        stmt.close();
        return stats;
    }

    @Override
    protected void processWindowElements(EmoteStats stats, Iterable<Emote> emotes) {
        stats.occurrences = 0;
        for (Emote emote : emotes) {
            stats.occurrences++;
            stats.totalOccurrences++;
        }
    }

    @Override
    public void prepareTable(Statement stmt) throws SQLException {
        stmt.execute("CREATE TABLE IF NOT EXISTS " + TABLE_NAME + "(" +
                "channel VARCHAR(32) NOT NULL," +
                "emote VARCHAR(64) NOT NULL," +
                "timestamp BIGINT NOT NULL," +
                "total_occurrences INT NOT NULL DEFAULT 0," +
                "occurrences INT NOT NULL DEFAULT 0," +
                "PRIMARY KEY(channel, emote, timestamp))");
    }

    @Override
    protected Iterable<OutputStatement> prepareStatsForOutput(EmoteStats stats) {
        String escapedEmote = escapeSingleQuotes(stats.emote);
        return OutputStatement.buildBatch()
                .add("INSERT INTO " + TABLE_NAME + "(timestamp, channel, emote, total_occurrences, occurrences) " +
                        "VALUES(" + stats.timestamp + ", '" + stats.channel + "', '" + escapedEmote + "', " + stats.totalOccurrences + ", " + stats.occurrences + ") " +
                        "ON CONFLICT(channel, emote, timestamp) DO UPDATE " +
                        "SET total_occurrences = excluded.total_occurrences, occurrences = excluded.occurrences")
                .add("INSERT INTO " + TABLE_NAME + "(timestamp, channel, emote, total_occurrences, occurrences) " +
                        "VALUES(0, '" + stats.channel + "', '" + escapedEmote + "', " + stats.totalOccurrences + ", " + stats.occurrences + ") " +
                        "ON CONFLICT(channel, emote, timestamp) DO UPDATE " +
                        "SET total_occurrences = excluded.total_occurrences, occurrences = excluded.occurrences")
                .finish();
    }
}
