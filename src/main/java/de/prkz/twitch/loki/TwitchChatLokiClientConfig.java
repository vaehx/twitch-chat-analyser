package de.prkz.twitch.loki;

import de.prkz.config.AbstractPropertiesConfig;
import de.prkz.config.InvalidConfigException;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;

public class TwitchChatLokiClientConfig extends AbstractPropertiesConfig {

    public TwitchChatLokiClientConfig(Path propertiesFile) throws InvalidConfigException, IOException {
        super(propertiesFile);
    }


    public static final String BOT_CHANNELS_KEY = "bot.channels";

    public String[] getBotChannels() throws InvalidConfigException {
        final var str = getNonEmptyString(BOT_CHANNELS_KEY);
        return str.split("\\s*,\\s*");
    }


    public static final String LOKI_BASE_URL_KEY = "loki.baseUrl";

    public URI getLokiBaseUrl() throws InvalidConfigException {
        return getUri(LOKI_BASE_URL_KEY);
    }

    public static final String LOKI_SEND_TIMEOUT_KEY = "loki.httpTimeout";
    public static final Duration DEFAULT_LOKI_SEND_TIMEOUT = Duration.ofSeconds(30);

    public Duration getLokiSendTimeout() {
        return getDuration(LOKI_SEND_TIMEOUT_KEY, DEFAULT_LOKI_SEND_TIMEOUT);
    }

    public static final String LOKI_BASIC_AUTH_KEY_PREFIX = "loki.basicAuth";

    public boolean getLokiBasicAuthEnabled() {
        return !getSubProperties(LOKI_BASIC_AUTH_KEY_PREFIX).isEmpty();
    }

    public static final String LOKI_BASIC_AUTH_USER_KEY = "loki.basicAuth.user";

    public String getLokiBasicAuthUser() {
        return getNonEmptyString(LOKI_BASIC_AUTH_USER_KEY);
    }

    public static final String LOKI_BASIC_AUTH_PASSWORD_KEY = "loki.basicAuth.password";

    public String getLokiBasicAuthPassword() {
        return getString(LOKI_BASIC_AUTH_PASSWORD_KEY);
    }

    public static final String LOKI_LINES_PER_BATCH_KEY = "loki.linesPerBatch";
    public static final int DEFAULT_LOKI_LINES_PER_BATCH = 100;

    /**
     * Number of lines (across all streams) after which a batch will be sent to loki
     */
    public int getLokiLinesPerBatch() {
        return getInt(LOKI_LINES_PER_BATCH_KEY, DEFAULT_LOKI_LINES_PER_BATCH);
    }

    public static final String LOKI_MAX_BATCH_WAIT_KEY = "loki.maxBatchWait";
    public static final Duration DEFAULT_LOKI_MAX_BATCH_WAIT = Duration.ofSeconds(10);

    /**
     * Max time to wait between sending batches before forcing push of a new batch even if the configured number of
     * lines was not reached yet. This configures a minimum for the end-to-end latency.
     */
    public Duration getLokiMaxBatchWait() {
        return getDuration(LOKI_MAX_BATCH_WAIT_KEY, DEFAULT_LOKI_MAX_BATCH_WAIT);
    }


    @Override
    protected void validate() throws InvalidConfigException {

    }
}
