package de.prkz.twitch.emoteanalyser.emote;

import de.prkz.twitch.emoteanalyser.AbstractStatsAggregation;
import de.prkz.twitch.emoteanalyser.output.OutputStatement;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;

import java.sql.*;

// Key: (channel, emote, username)
public class UserEmoteStatsAggregation
        extends AbstractStatsAggregation<Emote, Tuple3<String, String, String>, UserEmoteStats> {

    private static final String TABLE_NAME = "user_emote_stats";

    public UserEmoteStatsAggregation(String jdbcUrl,
                                     int dbBatchInterval,
                                     long aggregationIntervalMillis,
                                     long triggerIntervalMillis) {
        super(jdbcUrl, dbBatchInterval, aggregationIntervalMillis, triggerIntervalMillis);
    }

    @Override
    protected TypeInformation<UserEmoteStats> getStatsTypeInfo() {
        return TypeInformation.of(new TypeHint<UserEmoteStats>() {
        });
    }

    @Override
    protected KeySelector<Emote, Tuple3<String, String, String>> createKeySelector() {
        return new KeySelector<Emote, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> getKey(Emote emote) throws Exception {
                return new Tuple3<>(emote.channel, emote.emote, emote.username);
            }
        };
    }

    @Override
    protected UserEmoteStats createNewStatsForKey(Tuple3<String, String, String> key) throws SQLException {
        UserEmoteStats stats = new UserEmoteStats();
        stats.channel = key.f0;
        stats.emote = key.f1;
        stats.username = key.f2;

        // Load current count from database, if it exists
        Statement stmt = conn.createStatement();
        ResultSet result = stmt.executeQuery("SELECT total_occurrences, occurrences, timestamp FROM " + TABLE_NAME + " WHERE " +
                "channel='" + stats.channel + "' AND emote='" + stats.emote + "' AND username='" + stats.username + "' " +
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
    protected void processWindowElements(UserEmoteStats stats, Iterable<Emote> emotes) {
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
                "username VARCHAR(32) NOT NULL," +
                "timestamp BIGINT NOT NULL," +
                "total_occurrences INT NOT NULL DEFAULT 0," +
                "occurrences INT NOT NULL DEFAULT 0," +
                "PRIMARY KEY(channel, emote, username, timestamp))");
    }

    @Override
    protected Iterable<OutputStatement> prepareStatsForOutput(UserEmoteStats stats) {
        return OutputStatement.buildBatch()
                .add("INSERT INTO " + TABLE_NAME + "(timestamp, channel, emote, username, total_occurrences, occurrences) " +
                        "VALUES(" + stats.timestamp + ", '" + stats.channel + "', '" + stats.emote + "', '" + stats.username + "', " + stats.totalOccurrences + ", " + stats.occurrences + ") " +
                        "ON CONFLICT(channel, emote, username, timestamp) DO UPDATE " +
                        "SET total_occurrences = excluded.total_occurrences, occurrences = excluded.occurrences")
                .add("INSERT INTO " + TABLE_NAME + "(timestamp, channel, emote, username, total_occurrences, occurrences) " +
                        "VALUES(0, '" + stats.channel + "', '" + stats.emote + "', '" + stats.username + "', " + stats.totalOccurrences + ", " + stats.occurrences + ") " +
                        "ON CONFLICT(channel, emote, username, timestamp) DO UPDATE " +
                        "SET total_occurrences = excluded.total_occurrences, occurrences = excluded.occurrences")
                .finish();
    }
}
