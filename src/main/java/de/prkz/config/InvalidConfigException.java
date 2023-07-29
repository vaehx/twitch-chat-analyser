package de.prkz.config;

public class InvalidConfigException extends RuntimeException {
    public InvalidConfigException(String msg) {
        super(msg);
    }

    public InvalidConfigException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
