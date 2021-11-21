package de.prkz.twitch.emoteanalyser.config;

import java.nio.file.Path;

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
}
