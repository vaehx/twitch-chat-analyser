package de.prkz.twitch.emoteanalyser.emote.provider;

import de.prkz.twitch.emoteanalyser.emote.Channel;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.Duration;
import java.util.Map;

public abstract class EmoteProvider {

    protected final Duration fetchTimeout;

    protected EmoteProvider(Duration fetchTimeout) {
        this.fetchTimeout = fetchTimeout;
    }


    public abstract EmoteFetchResult fetchGlobalEmotes() throws Exception;
    public abstract EmoteFetchResult fetchChannelEmotes(Channel channel) throws Exception;


    protected String getJSONHttp(URL url, Map<String, String> additionalHeaders)
            throws NotFoundHttpException, Exception {
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setConnectTimeout((int)fetchTimeout.toMillis());
        con.setReadTimeout((int)fetchTimeout.toMillis());
        con.setRequestMethod("GET");
        con.setRequestProperty("Content-Type", "application/json");

        if (additionalHeaders != null)
            additionalHeaders.forEach((key, value) -> con.setRequestProperty(key, value));

        int status = con.getResponseCode();
        if (status == 404) {
            throw new NotFoundHttpException("404 - Not Found");
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
