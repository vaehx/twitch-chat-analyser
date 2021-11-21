package de.prkz.twitch.emoteanalyser.emote.provider;

import de.prkz.twitch.emoteanalyser.emote.Channel;
import de.prkz.twitch.emoteanalyser.emote.EmoteType;

import java.util.Set;

public class EmoteFetchResult {

    private final EmoteType type;
    private final Set<String> emotes;
    private final Channel channel;

    EmoteFetchResult(EmoteType type, Set<String> emotes, Channel channel) {
        if (type == null)
            throw new NullPointerException("type is null");

        if (emotes == null)
            throw new NullPointerException("emotes is null");

        this.type = type;
        this.emotes = emotes;
        this.channel = channel;
    }

    EmoteFetchResult(EmoteType type, Set<String> emotes) {
        this(type, emotes, null);
    }

    public EmoteType getEmoteType() {
        return type;
    }

    public Set<String> getEmotes() {
        return emotes;
    }

    public Channel getChannel() {
        return channel;
    }
}
