package de.prkz.twitch.emoteanalyser.bot.config;

public class InvalidConfigException extends Exception {
    public InvalidConfigException(String msg) {
        super(msg);
    }

    public InvalidConfigException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
