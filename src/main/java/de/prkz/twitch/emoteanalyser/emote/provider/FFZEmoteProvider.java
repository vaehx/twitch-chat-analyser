package de.prkz.twitch.emoteanalyser.emote.provider;

import de.prkz.twitch.emoteanalyser.emote.Channel;
import de.prkz.twitch.emoteanalyser.emote.EmoteType;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.net.URL;
import java.util.HashSet;
import java.util.Set;

public class FFZEmoteProvider extends EmoteProvider {

    private static final String FFZ_API_BASEURL = "https://api.frankerfacez.com/v1";


    public FFZEmoteProvider(int fetchTimeoutMillis) {
        super(fetchTimeoutMillis);
    }

    @Override
    public EmoteFetchResult fetchGlobalEmotes() throws Exception {
        URL url = new URL(FFZ_API_BASEURL + "/set/global");
        String response = getJSONHttp(url);

        Set<String> emotes = new HashSet<>();

        JSONObject responseObj = new JSONObject(response);
        if (!responseObj.has("default_sets"))
            throw new Exception("Invalid response: 'default_sets' object missing");

        if (!responseObj.has("sets"))
            throw new Exception("Invalid response: 'sets' object missing");

        JSONArray defaultSetsArr = responseObj.getJSONArray("default_sets");
        JSONObject setsObj = responseObj.getJSONObject("sets");
        for (int i = 0; i < defaultSetsArr.length(); ++i) {
            int defaultSetId = defaultSetsArr.getInt(i);
            JSONObject setObj = setsObj.getJSONObject(String.valueOf(defaultSetId));
            emotes.addAll(parseEmotesFromSet(setObj));
        }

        return new EmoteFetchResult(EmoteType.FFZ, emotes);
    }

    @Override
    public EmoteFetchResult fetchChannelEmotes(Channel channel) throws Exception {
        URL url = new URL(FFZ_API_BASEURL + "/room/" + channel.name);
        String response = getJSONHttp(url);

        Set<String> emotes = new HashSet<>();

        JSONObject responseObj = new JSONObject(response);
        if (!responseObj.has("sets"))
            throw new Exception("Invalid response: 'sets' object missing");

        JSONObject sets = responseObj.getJSONObject("sets");
        for (String setId : sets.keySet()) {
            JSONObject setObj = sets.getJSONObject(setId);
            emotes.addAll(parseEmotesFromSet(setObj));
        }

        return new EmoteFetchResult(EmoteType.FFZ, emotes, channel);
    }


    private static Set<String> parseEmotesFromSet(JSONObject setObj) throws JSONException {
        if (!setObj.has("emoticons"))
            return new HashSet<>();

        Set<String> emotes = new HashSet<>();
        JSONArray emoticons = setObj.getJSONArray("emoticons");
        for (int i = 0; i < emoticons.length(); ++i) {
            JSONObject emoticon = emoticons.getJSONObject(i);
            emotes.add(emoticon.getString("name"));
        }

        return emotes;
    }
}
