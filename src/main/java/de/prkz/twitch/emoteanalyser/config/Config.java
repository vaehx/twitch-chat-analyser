package de.prkz.twitch.emoteanalyser.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class Config {
    protected Properties props = new Properties();

    public static final String KEY_TWITCH_CLIENT_ID = "twitch.client-id";
    public String getTwitchClientId() {
        return props.getProperty(KEY_TWITCH_CLIENT_ID);
    }

    public static final String KEY_TWITCH_CLIENT_SECRET = "twitch.client-secret";
    public String getTwitchClientSecret() {
        return props.getProperty(KEY_TWITCH_CLIENT_SECRET);
    }

    public static final String KEY_DB_JDBC_URL = "db.jdbc-url";
    public String getDbJdbcUrl() {
        return props.getProperty(KEY_DB_JDBC_URL);
    }

    public static final String KEY_STREAMS_TABLE_NAME = "streams.table-name";
    public String getStreamsTableName() {
        return props.getProperty(KEY_STREAMS_TABLE_NAME);
    }

    public static final String KEY_STREAMS_UPDATE_COOLDOWN_MILLIS = "streams.update-cooldown.millis";
    public Long getStreamsUpdateCooldownMillis() {
        return Long.parseLong(props.getProperty(KEY_STREAMS_UPDATE_COOLDOWN_MILLIS));
    }

    public static final String KEY_KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap-servers";
    public String getKafkaBootstrapServers() {
        return props.getProperty(KEY_KAFKA_BOOTSTRAP_SERVERS);
    }

    public static final String KEY_KAFKA_TOPIC = "kafka.topic";
    public String getKafkaTopic() {
        return props.getProperty(KEY_KAFKA_TOPIC);
    }

    public static final String KEY_CHANNELS = "channels";
    public List<String> getChannels() {
        String channelStr = props.getProperty(KEY_CHANNELS);
        String[] channels = channelStr.split("\\s*,\\s*");
        return Arrays.asList(channels);
    }

    public static final String KEY_AGGREGATION_INTERVAL_MILLIS = "aggregation-interval.millis";
    public int getAggregationIntervalMillis() {
        return Integer.parseInt(props.getProperty(KEY_AGGREGATION_INTERVAL_MILLIS));
    }

    public static final String KEY_TRIGGER_INTERVAL_MILLIS = "trigger-interval.millis";
    public int getTriggerIntervalMillis() {
        return Integer.parseInt(props.getProperty(KEY_TRIGGER_INTERVAL_MILLIS));
    }


    private void validate() throws InvalidConfigException {
        if (getTwitchClientId() == null || getTwitchClientId().isEmpty())
            throw new InvalidConfigException("Missing or invalid " + KEY_TWITCH_CLIENT_ID);

        if (getTwitchClientSecret() == null || getTwitchClientSecret().isEmpty())
            throw new InvalidConfigException("Missing or invalid " + KEY_TWITCH_CLIENT_SECRET);

        if (getDbJdbcUrl() == null || getDbJdbcUrl().isEmpty())
            throw new InvalidConfigException("Missing or invalid " + KEY_DB_JDBC_URL);

        if (getStreamsTableName() == null || getStreamsTableName().isEmpty())
            throw new InvalidConfigException("Missing or invalid " + KEY_STREAMS_TABLE_NAME);

        if (getStreamsUpdateCooldownMillis() == null || getStreamsUpdateCooldownMillis() <= 0)
            throw new InvalidConfigException("Missing or invalid " + KEY_STREAMS_UPDATE_COOLDOWN_MILLIS);

        if (getKafkaBootstrapServers() == null || getKafkaBootstrapServers().isEmpty())
            throw new InvalidConfigException("Missing or invalid " + KEY_KAFKA_BOOTSTRAP_SERVERS);

        if (getKafkaTopic() == null || getKafkaTopic().isEmpty())
            throw new InvalidConfigException("Missing or invalid " + KEY_KAFKA_TOPIC);

        if (getChannels() == null || getChannels().isEmpty())
            throw new InvalidConfigException("Missing or invalid " + KEY_CHANNELS);
    }

    public static Config parse(Path path) throws InvalidConfigException {
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(path.toString()));
        } catch (IOException e) {
            throw new InvalidConfigException("Could not read config file: " + path, e);
        }

        Config cfg = new Config();
        cfg.props = properties;
        cfg.validate();
        return cfg;
    }
}
