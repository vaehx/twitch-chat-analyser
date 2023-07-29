package de.prkz.twitch.emoteanalyser.config;

import de.prkz.config.AbstractPropertiesConfig;
import de.prkz.config.InvalidConfigException;

import java.io.IOException;
import java.nio.file.Path;

public class CommonConfig extends AbstractPropertiesConfig {

    public CommonConfig(Path propertiesFile) throws InvalidConfigException, IOException {
        super(propertiesFile);
    }


    public static final String KEY_TWITCH_CLIENT_ID = "twitch.client-id";
    public String getTwitchClientId() throws InvalidConfigException {
        return getNonEmptyString(KEY_TWITCH_CLIENT_ID);
    }

    public static final String KEY_TWITCH_CLIENT_SECRET = "twitch.client-secret";
    public String getTwitchClientSecret() {
        return getNonEmptyString(KEY_TWITCH_CLIENT_SECRET);
    }


    public static final String KEY_DB_JDBC_URL = "db.jdbc-url";
    public String getDbJdbcUrl() {
        return getNonEmptyString(KEY_DB_JDBC_URL);
    }

    public static final String KEY_DB_CHANNELS_TABLE_NAME = "db.channels.table-name";
    public static final String DEFAULT_DB_CHANNELS_TABLE_NAME = "channels";
    public String getDbChannelsTableName() {
        return getNonEmptyString(KEY_DB_CHANNELS_TABLE_NAME, DEFAULT_DB_CHANNELS_TABLE_NAME);
    }

    public static final String KEY_KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap-servers";
    public String getKafkaBootstrapServers() {
        return getNonEmptyString(KEY_KAFKA_BOOTSTRAP_SERVERS);
    }

    public static final String KEY_KAFKA_TOPIC = "kafka.topic";
    public String getKafkaTopic() {
        return getNonEmptyString(KEY_KAFKA_TOPIC);
    }

    protected void validate() throws InvalidConfigException {
    }
}
