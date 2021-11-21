package de.prkz.twitch.emoteanalyser.emote.provider;

import de.prkz.twitch.emoteanalyser.emote.Channel;
import de.prkz.twitch.emoteanalyser.emote.EmoteType;
import org.json.JSONArray;
import org.json.JSONObject;

import java.net.URL;
import java.util.HashSet;
import java.util.Set;

public class SevenTVEmoteProvider extends EmoteProvider {

    private static final String SEVENTV_API_BASEURL = "https://api.7tv.app/v2";


    public SevenTVEmoteProvider(int fetchTimeoutMillis) {
        super(fetchTimeoutMillis);
    }

    @Override
    public EmoteFetchResult fetchGlobalEmotes() throws Exception {
        String response = getJSONHttp(new URL(SEVENTV_API_BASEURL + "/emotes/global"));
        Set<String> emotes = parseEmotesList(new JSONArray(response));
        return new EmoteFetchResult(EmoteType.SEVENTV, emotes);
    }

    @Override
    public EmoteFetchResult fetchChannelEmotes(Channel channel) throws Exception {
        String response = getJSONHttp(new URL(SEVENTV_API_BASEURL + "/users/" + channel.name + "/emotes"));
        Set<String> emotes = parseEmotesList(new JSONArray(response));
        return new EmoteFetchResult(EmoteType.SEVENTV, emotes, channel);
    }

    private static Set<String> parseEmotesList(JSONArray emotesArr) {
        Set<String> emotes = new HashSet<>();
        for (int i = 0; i < emotesArr.length(); ++i) {
            JSONObject emoteObj = emotesArr.getJSONObject(i);
            String emote = emoteObj.getString("name");
            emotes.add(emote);
        }

        return emotes;
    }
}
