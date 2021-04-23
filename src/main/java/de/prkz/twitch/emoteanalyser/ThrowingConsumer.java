package de.prkz.twitch.emoteanalyser;

@FunctionalInterface
public interface ThrowingConsumer<T> {
    void apply(T t) throws Exception;
}
