package de.prkz.config;

import com.fasterxml.jackson.databind.annotation.JsonAppend;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Map.entry;

public abstract class AbstractPropertiesConfig {

    protected final Properties properties;


    public AbstractPropertiesConfig(Path propertiesFile) throws InvalidConfigException, IOException {
        properties = new Properties();
        try (final var inputStream = new FileInputStream(propertiesFile.toString())) {
            properties.load(inputStream);
        } catch (IOException e) {
            throw new IOException("Could not read config file: " + propertiesFile, e);
        }

        validate();
    }

    public AbstractPropertiesConfig(Properties properties) throws InvalidConfigException {
        if (properties == null)
            throw new IllegalArgumentException("properties is null");

        this.properties = properties;

        validate();
    }

    protected abstract void validate() throws InvalidConfigException;

    public Properties getProperties() {
        return properties;
    }

    public Properties getSubProperties(String prefix) throws InvalidConfigException {
        requireKey(prefix);
        final var subProperties = new Properties();
        for (final var key : properties.stringPropertyNames()) {
            if (!key.startsWith(prefix))
                continue;

            subProperties.setProperty(key.substring(prefix.length()), properties.getProperty(key));
        }

        return subProperties;
    }

    public String getString(String key) throws InvalidConfigException {
        requireKey(key);
        return properties.getProperty(key);
    }

    public String getString(String key, String fallback) {
        return properties.getProperty(key, fallback);
    }

    public String getNonEmptyString(String key) throws InvalidConfigException {
        requireKey(key);
        final var str = properties.getProperty(key);
        if (str.isBlank())
            throw new InvalidConfigException("String at key '" + key + "' is empty/blank");
        return str;
    }

    public String getNonEmptyString(String key, String fallback) throws InvalidConfigException {
        return hasKey(key) ? getNonEmptyString(key) : fallback;
    }

    public URI getUri(String key) throws InvalidConfigException {
        requireKey(key);
        try {
            return URI.create(properties.getProperty(key));
        } catch (IllegalArgumentException e) {
            throw new InvalidConfigException("Invalid URI at key '" + key + "'");
        }
    }

    public URI getUri(String key, URI fallback) throws InvalidConfigException {
        return hasKey(key) ? getUri(key) : fallback;
    }

    public int getInt(String key) throws InvalidConfigException {
        requireKey(key);
        try {
            return Integer.parseInt(properties.getProperty(key));
        } catch (NumberFormatException e) {
            throw new InvalidConfigException("Invalid int at key '" + key + "'", e);
        }
    }

    public int getInt(String key, int fallback) throws InvalidConfigException {
        return hasKey(key) ? getInt(key) : fallback;
    }

    public boolean getBoolean(String key) throws InvalidConfigException {
        requireKey(key);
        final var str = properties.getProperty(key);
        if (str.equalsIgnoreCase("true"))
            return true;
        else if (str.equalsIgnoreCase("false"))
            return false;
        else
            throw new InvalidConfigException("Invalid boolean at key '" + key + "': " + str);
    }

    public boolean getBoolean(String key, boolean fallback) throws InvalidConfigException {
        return hasKey(key) ? getBoolean(key) : fallback;
    }


    public static final Pattern DURATION_PATTERN = Pattern.compile("^(?<amount>\\d+) *(?<unit>(ms|s|min|h|d|w|m|y))$");

    public static final Map<String, ChronoUnit> TEMPORAL_UNITS = Map.ofEntries(
        entry("ms", ChronoUnit.MILLIS),
        entry("s", ChronoUnit.SECONDS),
        entry("min", ChronoUnit.MINUTES),
        entry("h", ChronoUnit.HOURS),
        entry("d", ChronoUnit.DAYS),
        entry("w", ChronoUnit.WEEKS),
        entry("m", ChronoUnit.MONTHS),
        entry("y", ChronoUnit.YEARS)
    );

    /**
     * Assumes that 1 month = 30 days, 1 year = 365 days
     */
    protected Duration getDuration(String key) throws InvalidConfigException {
        String s = getNonEmptyString(key);
        Matcher m = DURATION_PATTERN.matcher(s.toLowerCase().trim());
        if (!m.matches())
            throw new InvalidConfigException("Invalid duration format at key '" + key + "'");

        String unit = m.group("unit");
        if (!TEMPORAL_UNITS.containsKey(unit))
            throw new InvalidConfigException("Unknown duration unit at key '" + key + "'");

        final long amount = Long.parseLong(m.group("amount"));
        final ChronoUnit chronoUnit = TEMPORAL_UNITS.get(unit);
        switch (chronoUnit) {
            // Date units are considered estimated and cannot be used for Duration#of(), so we have to map them manually
            case DAYS:
                return Duration.ofDays(amount);
            case WEEKS:
                return Duration.ofDays(7 * amount);
            case MONTHS:
                return Duration.ofDays(30 * amount);
            case YEARS:
                return Duration.ofDays(365 * amount);
            default:
                return Duration.of(amount, TEMPORAL_UNITS.get(unit));
        }
    }

    /**
     * @see AbstractPropertiesConfig#getDuration(String)
     */
    protected Duration getDuration(String key, Duration fallback) throws InvalidConfigException {
        return hasKey(key) ? getDuration(key) : fallback;
    }


    private boolean hasKey(String key) {
        return properties.containsKey(key);
    }

    private void requireKey(String key) throws InvalidConfigException {
        if (!properties.containsKey(key))
            throw new InvalidConfigException("Missing key '" + key + "'");
    }
}
