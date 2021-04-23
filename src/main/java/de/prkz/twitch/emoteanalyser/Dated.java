package de.prkz.twitch.emoteanalyser;

/**
 * An element that tracks last update time of its value (which is of type T)
 */
public class Dated<T> {
    private long updatedAt;
    private T t;

    public Dated() {
        updatedAt = 0;
        t = null;
    }

    public Dated(T t) {
        set(t);
    }

    public T get() {
        return t;
    }

    /**
     * @return the time this value was last updated ({@link System#currentTimeMillis()}
     */
    public long updatedAt() {
        return updatedAt;
    }

    public void set(T t) {
        this.updatedAt = System.currentTimeMillis();
        this.t = t;
    }
}
