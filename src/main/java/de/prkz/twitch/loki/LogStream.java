package de.prkz.twitch.loki;

import java.util.HashMap;
import java.util.Map;

public class LogStream {

    private final Map<String, String> labels;

    public LogStream(Map<String, String> labels) {
        if (labels == null)
            throw new IllegalArgumentException("labels is null");

        if (labels.isEmpty())
            throw new IllegalArgumentException("labels is empty");

        this.labels = labels;
    }

    public Map<String, String> getLabels() {
        return labels;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof LogStream))
            return false;

        final var other = (LogStream)obj;
        return labels.equals(other.labels);
    }

    @Override
    public int hashCode() {
        return labels.hashCode();
    }


    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final Map<String, String> labels = new HashMap<>();

        public Builder withLabel(String label, String value) {
            labels.put(label, value);
            return this;
        }

        public LogStream build() {
            return new LogStream(labels);
        }
    }
}
