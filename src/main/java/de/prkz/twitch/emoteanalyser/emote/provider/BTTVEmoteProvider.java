package de.prkz.twitch.emoteanalyser.emote.provider;

import de.prkz.twitch.emoteanalyser.emote.Channel;
import de.prkz.twitch.emoteanalyser.emote.EmoteType;
import org.json.JSONArray;
import org.json.JSONObject;

import java.net.URL;
import java.util.HashSet;
import java.util.Set;

public class BTTVEmoteProvider extends EmoteProvider {

    private static final String BTTV_API_BASEURL = "https://api.betterttv.net/2";


    public BTTVEmoteProvider(int fetchTimeoutMillis) {
        super(fetchTimeoutMillis);
    }

    @Override
    public EmoteFetchResult fetchGlobalEmotes() throws Exception {
        URL url = new URL(BTTV_API_BASEURL + "/emotes");
        String response = getJSONHttp(url);

        Set<String> emotes = parseBTTVEmotesResponse(response);

        return new EmoteFetchResult(EmoteType.BTTV, emotes);
    }

    @Override
    public EmoteFetchResult fetchChannelEmotes(Channel channel) throws Exception {
        URL url = new URL(BTTV_API_BASEURL + "/channels/" + channel.name);
        String response = getJSONHttp(url);

        Set<String> emotes = parseBTTVEmotesResponse(response);

        return new EmoteFetchResult(EmoteType.BTTV, emotes, channel);
    }


    private static Set<String> parseBTTVEmotesResponse(String response) throws Exception {
        JSONObject responseObj = new JSONObject(response);
        JSONArray emotesArr = responseObj.getJSONArray("emotes");
        if (emotesArr == null)
            throw new Exception("Response does not include emotes array");

        Set<String> emotes = new HashSet<>();
        for (int i = 0; i < emotesArr.length(); ++i) {
            JSONObject emoteObj = emotesArr.getJSONObject(i);
            emotes.add(emoteObj.getString("code"));
        }

        return emotes;
    }
}
