package de.prkz.twitch.emoteanalyser.emote.provider;

import de.prkz.twitch.emoteanalyser.emote.Channel;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;

public abstract class EmoteProvider {

    protected final int fetchTimeoutMillis;

    protected EmoteProvider(int fetchTimeoutMillis) {
        this.fetchTimeoutMillis = fetchTimeoutMillis;
    }


    public abstract EmoteFetchResult fetchGlobalEmotes() throws Exception;
    public abstract EmoteFetchResult fetchChannelEmotes(Channel channel) throws Exception;


    protected String getJSONHttp(URL url, Map<String, String> additionalHeaders) throws Exception {
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setConnectTimeout(fetchTimeoutMillis);
        con.setReadTimeout(fetchTimeoutMillis);
        con.setRequestMethod("GET");
        con.setRequestProperty("Content-Type", "application/json");

        if (additionalHeaders != null)
            additionalHeaders.forEach((key, value) -> con.setRequestProperty(key, value));

        int status = con.getResponseCode();
        if (status == 404) {
            throw new Exception("Not found (404)");
        } else if (status != 200) {
            throw new Exception("Got HTTP error for request to URL '" + url.toString() + "': " +
                    "Code " + con.getResponseCode() + ", Message: " + con.getResponseMessage());
        }

        BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
        String line;
        StringBuilder response = new StringBuilder();
        while ((line = in.readLine()) != null)
            response.append(line);

        in.close();
        con.disconnect();

        return response.toString();
    }

    protected String getJSONHttp(URL url) throws Exception {
        return getJSONHttp(url, null);
    }
}
