package de.prkz.twitch.emoteanalyser.config;

public class InvalidConfigException extends Exception {
    public InvalidConfigException(String msg) {
        super(msg);
    }

    public InvalidConfigException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
