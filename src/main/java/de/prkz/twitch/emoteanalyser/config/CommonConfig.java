package de.prkz.twitch.emoteanalyser.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;

public class CommonConfig {

    protected final Properties props;


    public CommonConfig(Path propertiesFile) throws InvalidConfigException {
        props = new Properties();
        try {
            props.load(new FileInputStream(propertiesFile.toString()));
        } catch (IOException e) {
            throw new InvalidConfigException("Could not read config file: " + propertiesFile, e);
        }

        validate();
    }

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

    public static final String KEY_DB_CHANNELS_TABLE_NAME = "db.channels.table-name";
    public String getDbChannelsTableName() {
        return props.getProperty(KEY_DB_CHANNELS_TABLE_NAME, "channels");
    }

    public static final String KEY_KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap-servers";
    public String getKafkaBootstrapServers() {
        return props.getProperty(KEY_KAFKA_BOOTSTRAP_SERVERS);
    }

    public static final String KEY_KAFKA_TOPIC = "kafka.topic";
    public String getKafkaTopic() {
        return props.getProperty(KEY_KAFKA_TOPIC);
    }

    protected void validate() throws InvalidConfigException {
        if (getTwitchClientId() == null || getTwitchClientId().isEmpty())
            throw new InvalidConfigException("Missing or invalid " + KEY_TWITCH_CLIENT_ID);

        if (getTwitchClientSecret() == null || getTwitchClientSecret().isEmpty())
            throw new InvalidConfigException("Missing or invalid " + KEY_TWITCH_CLIENT_SECRET);

        if (getDbJdbcUrl() == null || getDbJdbcUrl().isEmpty())
            throw new InvalidConfigException("Missing or invalid " + KEY_DB_JDBC_URL);

        if (getDbChannelsTableName() == null || getDbChannelsTableName().isEmpty())
            throw new InvalidConfigException("Missing or invalid " + KEY_DB_CHANNELS_TABLE_NAME);

        if (getKafkaBootstrapServers() == null || getKafkaBootstrapServers().isEmpty())
            throw new InvalidConfigException("Missing or invalid " + KEY_KAFKA_BOOTSTRAP_SERVERS);

        if (getKafkaTopic() == null || getKafkaTopic().isEmpty())
            throw new InvalidConfigException("Missing or invalid " + KEY_KAFKA_TOPIC);
    }
}
