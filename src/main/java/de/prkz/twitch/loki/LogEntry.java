package de.prkz.twitch.loki;

import java.time.Instant;

public class LogEntry {

    private final Instant timestamp;
    private final String line;

    public LogEntry(Instant timestamp, String line) {
        if (timestamp == null)
            throw new IllegalArgumentException("timestamp is null");

        if (line == null)
            throw new IllegalArgumentException("line is null");

        this.timestamp = timestamp;
        this.line = line;
    }

    /** Log entry at current time */
    public LogEntry(String line) {
        this(Instant.now(), line);
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public String getLine() {
        return line;
    }
}
