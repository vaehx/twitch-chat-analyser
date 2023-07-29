package de.prkz.twitch.emoteanalyser;

import java.time.Instant;

/**
 * An element that tracks last update time of its value (which is of type T)
 */
public class Dated<T> {

    private Instant updatedAt;
    private T t;

    public Dated() {
        updatedAt = Instant.ofEpochMilli(0);
        t = null;
    }

    public Dated(T t) {
        set(t);
    }

    public T get() {
        return t;
    }

    /**
     * @return the time this value was last updated; never {@code null}
     */
    public Instant updatedAt() {
        return updatedAt;
    }

    public void set(T t) {
        this.updatedAt = Instant.now();
        this.t = t;
    }
}
