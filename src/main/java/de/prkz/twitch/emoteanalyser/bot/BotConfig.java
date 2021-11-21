package de.prkz.twitch.emoteanalyser.bot;

import de.prkz.twitch.emoteanalyser.config.CommonConfig;
import de.prkz.twitch.emoteanalyser.config.InvalidConfigException;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

public class BotConfig extends CommonConfig {

    public BotConfig(Path propertiesFile) throws InvalidConfigException {
        super(propertiesFile);
    }

    public static final String KEY_STREAMS_TABLE_NAME = "streams.table-name";
    public String getStreamsTableName() {
        return props.getProperty(KEY_STREAMS_TABLE_NAME);
    }

    public static final String KEY_STREAMS_UPDATE_COOLDOWN_MILLIS = "streams.update-cooldown.millis";
    public Long getStreamsUpdateCooldownMillis() {
        return Long.parseLong(props.getProperty(KEY_STREAMS_UPDATE_COOLDOWN_MILLIS));
    }

    public static final String KEY_CHANNELS = "channels";
    public List<String> getChannels() {
        String channelStr = props.getProperty(KEY_CHANNELS);
        String[] channels = channelStr.split("\\s*,\\s*");
        return Arrays.asList(channels);
    }

    @Override
    protected void validate() throws InvalidConfigException {
        super.validate();

        if (getStreamsTableName() == null || getStreamsTableName().isEmpty())
            throw new InvalidConfigException("Missing or invalid " + KEY_STREAMS_TABLE_NAME);

        if (getStreamsUpdateCooldownMillis() == null || getStreamsUpdateCooldownMillis() <= 0)
            throw new InvalidConfigException("Missing or invalid " + KEY_STREAMS_UPDATE_COOLDOWN_MILLIS);

        if (getChannels() == null || getChannels().isEmpty())
            throw new InvalidConfigException("Missing or invalid " + KEY_CHANNELS);
    }
}
