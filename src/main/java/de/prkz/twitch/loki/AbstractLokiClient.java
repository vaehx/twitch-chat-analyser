package de.prkz.twitch.loki;

import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.HttpEntities;
import org.apache.hc.core5.net.URIBuilder;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public abstract class AbstractLokiClient implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractLokiClient.class);

    private final URI baseUri;
    private final CloseableHttpClient httpClient;

    public AbstractLokiClient(URI baseUri, Duration httpRequestTimeout) {
        if (baseUri == null)
            throw new IllegalArgumentException("baseUri is null");

        this.baseUri = baseUri;

        final var requestConfig = RequestConfig.custom()
                .setResponseTimeout(httpRequestTimeout.toMillis(), TimeUnit.MILLISECONDS)
                .build();

        httpClient = HttpClientBuilder.create()
                .setDefaultRequestConfig(requestConfig)
                .build();
    }

    public void pushLogStreams(Map<LogStream, ? extends Iterable<LogEntry>> streams) throws IOException {
        if (streams == null)
            throw new IllegalArgumentException("logEntries is null");

        if (streams.isEmpty())
            return;

        final var uri = fromBaseUri().appendPath("/loki/api/v1/push");

        final var streamsArr = new JSONArray();
        for (final var stream : streams.keySet()) {
            final var logEntries = streams.get(stream);
            if (logEntries == null)
                continue;

            final var valuesArr = new JSONArray();
            long n = 0;
            for (final var entry : logEntries) {
                final var ts = entry.getTimestamp();
                valuesArr.put(Arrays.asList(
                        toLokiTimestamp(ts),
                        entry.getLine()
                ));
                ++n;
            }

            if (n == 0)
                continue;

            final var streamObj = new JSONObject();
            streamObj.put("stream", stream.getLabels());
            streamObj.put("values", valuesArr);
            streamsArr.put(streamObj);
        }

        final var reqObj = new JSONObject();
        reqObj.put("streams", streamsArr);

        post(uri, reqObj);
    }



    private URIBuilder fromBaseUri() {
        return new URIBuilder(baseUri);
    }

    private static HttpPost newPostRequest(URIBuilder uriBuilder) throws IOException {
        URI uri;
        try {
            uri = uriBuilder.build();
        } catch (URISyntaxException e) {
            throw new IOException("Could not build URI for POST request", e);
        }

        LOG.info("Sending POST request to {}", uri);

        return new HttpPost(uri);
    }

    private void post(URIBuilder uriBuilder, JSONObject json) throws IOException {
        final var request = newPostRequest(uriBuilder);
        request.setHeader("Content-Type", "application/json");
        request.setEntity(HttpEntities.create(json.toString()));

        httpClient.execute(request, response -> {
            if (!isSuccessCode(response.getCode())) {
                final var responseBody = EntityUtils.toString(response.getEntity());
                throw new IOException("Error response from server: " +
                        "Code " + response.getCode() + ", Body: " + responseBody);
            }

            return null;
        });
    }

    @Override
    public void close() throws IOException {
        httpClient.close();
    }


    private static boolean isSuccessCode(int code) {
        return 200 <= code && code < 300;
    }

    static String toLokiTimestamp(Instant instant) {
        return instant.getEpochSecond() + String.format("%09d", instant.getNano());
    }
}
