package de.prkz.twitch.emoteanalyser.emote;

public class Channel {

    public String name;
    public String broadcasterId;

    public Channel(String name, String broadcasterId) {
        if (name == null)
            throw new IllegalArgumentException("name is null");

        this.name = name;
        this.broadcasterId = broadcasterId;
    }

    public Channel(String name) {
        this(name, null);
    }

    @Override
    public String toString() {
        return name;
    }
}
