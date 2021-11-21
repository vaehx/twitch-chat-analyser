package de.prkz.twitch.emoteanalyser.emote.provider;

import de.prkz.twitch.emoteanalyser.emote.Channel;
import de.prkz.twitch.emoteanalyser.emote.EmoteType;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.*;

public class TwitchEmoteProvider extends EmoteProvider {

    private static final Logger LOG = LoggerFactory.getLogger(TwitchEmoteProvider.class);

    private static final String TWITCH_KRAKEN_API_BASEURL = "https://api.twitch.tv/kraken";

    private final String twitchClientId;


    public TwitchEmoteProvider(String twitchClientId, int fetchTimeoutMillis) {
        super(fetchTimeoutMillis);
        this.twitchClientId = twitchClientId;
    }

    @Override
    public EmoteFetchResult fetchGlobalEmotes() throws Exception {
        Set<String> emotes = fetchEmoteSets(Arrays.asList(
                0, /* Global emotes */
                793, /* Twitch Turbo */
                19194, /* Twitch Prime */
                695138 /* OWL All Access Pass */));

        return new EmoteFetchResult(EmoteType.TWITCH_GLOBAL, emotes);
    }

    @Override
    public EmoteFetchResult fetchChannelEmotes(Channel channel) throws Exception {
        Set<String> emotes = fetchEmoteSets(Collections.singletonList(channel.emoteSet));
        return new EmoteFetchResult(EmoteType.TWITCH_SUBSCRIBER, emotes, channel);
    }

    private Set<String> fetchEmoteSets(List<Integer> emoteSets) throws Exception {
        if (emoteSets.isEmpty())
            return new HashSet<>();

        StringBuilder emoteSetsParam = new StringBuilder();
        for (int i = 0; i < emoteSets.size(); ++i) {
            if (i > 0)
                emoteSetsParam.append(',');
            emoteSetsParam.append(emoteSets.get(i));
        }

        URL url = new URL(TWITCH_KRAKEN_API_BASEURL + "/chat/emoticon_images?emotesets=" + emoteSetsParam.toString());
        String response = getJSONHttp(url, new HashMap<String, String>() {{
            put("Client-ID", twitchClientId);
            put("Accept", "application/vnd.twitchtv.v5+json");
        }});

        JSONObject responseObj = new JSONObject(response);
        JSONObject emoticonSets = responseObj.getJSONObject("emoticon_sets");
        if (emoticonSets == null)
            throw new Exception("Response does not include emoticon_sets");

        Set<String> emotes = new HashSet<>();
        for (String emoteSetId : emoticonSets.keySet()) {
            JSONArray emoteSet = emoticonSets.getJSONArray(emoteSetId);
            for (int i = 0; i < emoteSet.length(); ++i) {
                JSONObject emote = emoteSet.getJSONObject(i);
                emotes.add(emote.getString("code"));
            }

            LOG.info("Fetched " + emoteSet.length() + " emotes in emote set #" + emoteSetId);
        }

        return emotes;
    }
}
