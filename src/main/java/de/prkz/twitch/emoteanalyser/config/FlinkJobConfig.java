package de.prkz.twitch.emoteanalyser.config;

import de.prkz.config.InvalidConfigException;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;

public class FlinkJobConfig extends CommonConfig {

    public FlinkJobConfig(Path propertiesFile) throws InvalidConfigException, IOException {
        super(propertiesFile);
    }

    public static final String KEY_AGGREGATION_INTERVAL = "aggregation-interval";
    public Duration getAggregationInterval() {
        return getDuration(KEY_AGGREGATION_INTERVAL);
    }

    public static final String KEY_TRIGGER_INTERVAL = "trigger-interval";
    public Duration getTriggerInterval() {
        return getDuration(KEY_TRIGGER_INTERVAL);
    }

    public static final String KEY_EMOTE_FETCH_INTERVAL = "emote-fetch.interval";
    public static final Duration DEFAULT_EMOTE_FETCH_INTERVAL = Duration.ofMinutes(5);
    public Duration getEmoteFetchInterval() {
        return getDuration(KEY_EMOTE_FETCH_INTERVAL, DEFAULT_EMOTE_FETCH_INTERVAL);
    }

    public static final String KEY_EMOTE_FETCH_TIMEOUT = "emote-fetch.timeout";
    public static final Duration DEFAULT_EMOTE_FETCH_TIMEOUT = Duration.ofSeconds(20);
    public Duration getEmoteFetchTimeout() {
        return getDuration(KEY_EMOTE_FETCH_TIMEOUT, DEFAULT_EMOTE_FETCH_TIMEOUT);
    }

    public static final String KEY_DB_EMOTES_TABLE_NAME = "db.emotes.table-name";
    public static final String DEFAULT_DB_EMOTES_TABLE_NAME = "emotes";
    public String getDbEmotesTableName() {
        return getNonEmptyString(KEY_DB_EMOTES_TABLE_NAME, DEFAULT_DB_EMOTES_TABLE_NAME);
    }

    @Override
    protected void validate() throws InvalidConfigException {
        super.validate();
        getDbEmotesTableName();
    }
}
