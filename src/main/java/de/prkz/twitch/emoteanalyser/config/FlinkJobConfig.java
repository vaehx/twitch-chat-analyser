package de.prkz.twitch.emoteanalyser.config;

import java.nio.file.Path;
import java.time.Duration;

public class FlinkJobConfig extends CommonConfig {

    public FlinkJobConfig(Path propertiesFile) throws InvalidConfigException {
        super(propertiesFile);
    }

    public static final String KEY_AGGREGATION_INTERVAL_MILLIS = "aggregation-interval.millis";
    public int getAggregationIntervalMillis() {
        return Integer.parseInt(props.getProperty(KEY_AGGREGATION_INTERVAL_MILLIS));
    }

    public static final String KEY_TRIGGER_INTERVAL_MILLIS = "trigger-interval.millis";
    public int getTriggerIntervalMillis() {
        return Integer.parseInt(props.getProperty(KEY_TRIGGER_INTERVAL_MILLIS));
    }

    public static final String KEY_EMOTE_FETCH_INTERVAL_MILLIS = "emote-fetch.interval.millis";
    public int getEmoteFetchIntervalMillis() {
        return Integer.parseInt(props.getProperty(KEY_EMOTE_FETCH_INTERVAL_MILLIS,
                String.valueOf(Duration.ofMinutes(5).toMillis())));
    }

    public static final String KEY_EMOTE_FETCH_TIMEOUT_MILLIS = "emote-fetch.timeout.millis";
    public int getEmoteFetchTimeoutMillis() {
        return Integer.parseInt(props.getProperty(KEY_EMOTE_FETCH_TIMEOUT_MILLIS,
                String.valueOf(Duration.ofSeconds(20).toMillis())));
    }

    public static final String KEY_DB_EMOTES_TABLE_NAME = "db.emotes.table-name";
    public String getDbEmotesTableName() {
        return props.getProperty(KEY_DB_EMOTES_TABLE_NAME, "emotes");
    }

    @Override
    protected void validate() throws InvalidConfigException {
        super.validate();

        if (getEmoteFetchIntervalMillis() <= 0)
            throw new InvalidConfigException("Invalid " + KEY_EMOTE_FETCH_INTERVAL_MILLIS);

        if (getEmoteFetchTimeoutMillis() <= 0)
            throw new InvalidConfigException("Invalid " + KEY_EMOTE_FETCH_TIMEOUT_MILLIS);

        if (getDbEmotesTableName() == null || getDbEmotesTableName().isEmpty())
            throw new InvalidConfigException("Missing " + KEY_DB_EMOTES_TABLE_NAME);
    }
}
