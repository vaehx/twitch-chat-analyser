package de.prkz.twitch.emoteanalyser.bot;

import de.prkz.twitch.emoteanalyser.config.CommonConfig;
import de.prkz.config.InvalidConfigException;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;

public class BotConfig extends CommonConfig {

    public BotConfig(Path propertiesFile) throws InvalidConfigException, IOException {
        super(propertiesFile);
    }

    public static final String KEY_STREAMS_TABLE_NAME = "streams.table-name";
    public String getStreamsTableName() {
        return getNonEmptyString(KEY_STREAMS_TABLE_NAME);
    }

    public static final String KEY_STREAMS_UPDATE_COOLDOWN = "streams.update-cooldown";
    public Duration getStreamsUpdateCooldown() {
        return getDuration(KEY_STREAMS_UPDATE_COOLDOWN);
    }

    public static final String KEY_CHANNELS = "channels";
    public List<String> getChannels() {
        final var str = getNonEmptyString(KEY_CHANNELS);
        return Arrays.asList(str.split("\\s*,\\s*"));
    }

    @Override
    protected void validate() throws InvalidConfigException {
        super.validate();
    }
}
