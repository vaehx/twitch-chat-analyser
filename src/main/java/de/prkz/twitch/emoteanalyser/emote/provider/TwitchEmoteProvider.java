package de.prkz.twitch.emoteanalyser.emote.provider;

import com.github.twitch4j.helix.TwitchHelix;
import com.github.twitch4j.helix.domain.Emote;
import com.github.twitch4j.helix.domain.EmoteList;
import com.netflix.hystrix.exception.HystrixRuntimeException;
import de.prkz.twitch.emoteanalyser.emote.Channel;
import de.prkz.twitch.emoteanalyser.emote.EmoteType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class TwitchEmoteProvider extends EmoteProvider {

    private static final Logger LOG = LoggerFactory.getLogger(TwitchEmoteProvider.class);

    private final TwitchHelix twitch;


    public TwitchEmoteProvider(TwitchHelix twitch, int fetchTimeoutMillis) {
        super(fetchTimeoutMillis);
        this.twitch = twitch;
    }

    @Override
    public EmoteFetchResult fetchGlobalEmotes() throws Exception {
        EmoteList emoteList;
        try {
            emoteList = twitch.getGlobalEmotes(null).execute();
        } catch (HystrixRuntimeException e) {
            // If this call fails, it is unlikely that the following additional calls will succeed too
            throw new Exception("Could not fetch global twitch emotes from Helix API", e);
        }

        Set<String> emotes = emoteList.getEmotes().stream()
                .map(Emote::getName)
                .collect(Collectors.toSet());

        // Fetch some additional "global" emote sets
        try {
            emotes.addAll(fetchEmoteSets(Arrays.asList(
                    0, /* Global emotes */
                    793, /* Twitch Turbo */
                    19194, /* Twitch Prime */
                    695138 /* OWL All Access Pass */)));
        } catch (Exception e) {
            LOG.warn("Could not fetch additional global emote sets", e);
        }

        return new EmoteFetchResult(EmoteType.TWITCH_GLOBAL, emotes);
    }

    @Override
    public EmoteFetchResult fetchChannelEmotes(Channel channel) throws Exception {
        if (channel == null)
            throw new IllegalArgumentException("channel is null");

        if (channel.broadcasterId == null)
            return null;

        EmoteList emoteList;
        try {
            emoteList = twitch.getChannelEmotes(null, channel.broadcasterId).execute();
        } catch (HystrixRuntimeException e) {
            throw new Exception("Could not fetch channel emotes for channel " + channel.name + ", " +
                    "broadcasterId = " + channel.broadcasterId, e);
        }

        Set<String> emotes = new HashSet<>();
        for (Emote emote : emoteList.getEmotes())
            emotes.add(emote.getName());

        return new EmoteFetchResult(EmoteType.TWITCH_SUBSCRIBER, emotes, channel);
    }

    private Set<String> fetchEmoteSets(List<Integer> emoteSets) throws Exception {
        if (emoteSets.isEmpty())
            return new HashSet<>();

        EmoteList emoteList;
        try {
            emoteList = twitch
                    .getEmoteSets(null, emoteSets.stream().map(Object::toString).collect(Collectors.toList()))
                    .execute();
        } catch (HystrixRuntimeException e) {
            throw new Exception("Could not fetch emote sets " +
                    emoteSets.stream().map(Object::toString).collect(Collectors.joining(",")), e);
        }

        Set<String> emotes = new HashSet<>();
        for (Emote emote : emoteList.getEmotes())
            emotes.add(emote.getName());

        return emotes;
    }
}
