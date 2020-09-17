package de.prkz.twitch.emoteanalyser.channel;

import de.prkz.twitch.emoteanalyser.AbstractStatsAggregation;
import de.prkz.twitch.emoteanalyser.Message;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public class ChannelStatsAggregation extends AbstractStatsAggregation<Message, String, ChannelStats> {

    private static final String TABLE_NAME = "channel_stats";

    public ChannelStatsAggregation(String jdbcUrl, long aggregationIntervalMillis, long triggerIntervalMillis) {
        super(jdbcUrl, aggregationIntervalMillis, triggerIntervalMillis);
    }


    @Override
    protected ChannelStats createNewStatsForKey(String channel) {
        ChannelStats stats = new ChannelStats();
        stats.channel = channel;
        return stats;
    }

    @Override
    protected ChannelStats aggregate(ChannelStats stats, Message element) {
        stats.messageCount++;
        return stats;
    }

    @Override
    public void prepareTable(Statement stmt) throws SQLException {
        stmt.execute("CREATE TABLE IF NOT EXISTS " + TABLE_NAME + "(" +
                "channel VARCHAR(32) NOT NULL," +
                "timestamp BIGINT NOT NULL," +
                "messages BIGINT NOT NULL," +
                "PRIMARY KEY(channel, timestamp))");
    }

    @Override
    protected String getUpsertSql() {
        return "INSERT INTO " + TABLE_NAME + "(timestamp, channel, messages) " +
                "VALUES (?, ?, ?), (?, ?, ?) " +
                "ON CONFLICT(channel, timestamp) DO UPDATE SET " +
                "messages = " + TABLE_NAME + ".messages + EXCLUDED.messages";
    }

    @Override
    protected void setFieldsForOutput(PreparedStatement stmt, ChannelStats stats) throws SQLException {
        // diff
        stmt.setLong(1, stats.timestamp);
        stmt.setString(2, stats.channel);
        stmt.setLong(3, stats.messageCount);

        // total
        stmt.setLong(4, LATEST_TOTAL_TIMESTAMP);
        stmt.setString(5, stats.channel);
        stmt.setLong(6, stats.messageCount);
    }

    @Override
    protected TypeInformation<Tuple2<String, Long>> getKeyTypeInfo() {
        return TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {});
    }

    @Override
    protected TypeInformation<ChannelStats> getStatsTypeInfo() {
        return TypeInformation.of(new TypeHint<ChannelStats>() {});
    }

    @Override
    protected long getTimestampForElement(Message message) {
        return message.timestamp;
    }

    @Override
    protected String getKeyForElement(Message message) {
        return message.channel;
    }

    @Override
    protected Integer getHashForElement(Message message) {
        return message.channel.hashCode();
    }
}
