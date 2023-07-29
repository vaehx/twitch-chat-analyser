package de.prkz.twitch.loki;

import com.github.twitch4j.TwitchClient;
import com.github.twitch4j.TwitchClientBuilder;
import com.github.twitch4j.chat.TwitchChat;
import com.github.twitch4j.chat.events.AbstractChannelMessageEvent;
import com.github.twitch4j.chat.events.channel.ChannelMessageActionEvent;
import com.github.twitch4j.chat.events.channel.ChannelMessageEvent;
import com.github.twitch4j.chat.events.channel.IRCMessageEvent;
import com.github.twitch4j.common.events.domain.EventChannel;
import com.github.twitch4j.common.events.domain.EventUser;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

public class TwitchChatLokiClient extends AbstractLokiClient {

    private static final Logger LOG = LoggerFactory.getLogger(TwitchChatLokiClient.class);

    private final TwitchChatLokiClientConfig config;
    private final TwitchClient twitch;
    private final TwitchChat chat;

    private final Map<LogStream, List<LogEntry>> logs = new HashMap<>();
    private long messagesSinceLastBatchSent = 0;
    private Instant lastBatchSent;

    public TwitchChatLokiClient(TwitchChatLokiClientConfig config) {
        super(config.getLokiBaseUrl(), config.getLokiSendTimeout());
        this.config = config;

        LOG.info("Starting send timer...");
        lastBatchSent = Instant.now();
        final var timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                sendBatchIfNecessary();
            }
        }, 0, config.getLokiMaxBatchWait().toMillis());

        LOG.info("Starting twitch client(s)...");
        twitch = TwitchClientBuilder.builder()
                .withEnableChat(true)
                .build();

        chat = twitch.getChat();

        for (var channel : config.getBotChannels()) {
            LOG.info("Will join channel '{}'", channel);
            chat.joinChannel(channel);
        }

        chat.getEventManager().onEvent(ChannelMessageEvent.class, this::onChatMessage);
        chat.getEventManager().onEvent(ChannelMessageActionEvent.class, this::onChatMessage);
    }

    private void onChatMessage(AbstractChannelMessageEvent event) {
        EventUser user = event.getUser();
        if (user == null)
            return;

        EventChannel channel = event.getChannel();
        if (channel == null)
            return;

        String messageText = event.getMessage();
        if (messageText == null)
            return;

        messageText = messageText.trim();
        if (messageText.isEmpty())
            return;

        if (event instanceof ChannelMessageActionEvent) {
            messageText = "/me " + messageText;
        }

        final var line = new JSONObject();
        line.put("user", user.getName());
        line.put("text", messageText);

        //LOG.info("{} - {}", event.getFiredAtInstant().toString(), line);

        final var stream = getStreamForTwitchChannel(channel);

        synchronized (logs) {
            var entries = logs.computeIfAbsent(stream, k -> new ArrayList<>());
            entries.add(new LogEntry(event.getFiredAtInstant(), line.toString()));
            ++messagesSinceLastBatchSent;
        }

        sendBatchIfNecessary();
    }

    private synchronized void sendBatchIfNecessary() {
        if (messagesSinceLastBatchSent >= config.getLokiLinesPerBatch()
                || Instant.now().isAfter(lastBatchSent.plus(config.getLokiMaxBatchWait()))) {
            sendBatchNow();
        }
    }

    private synchronized void sendBatchNow() {
        try {
            pushLogStreams(logs);
        } catch (IOException e) {
            LOG.error("Could not send batch to Loki. Will keep the data and try again later...", e);
            return;
        } finally {
            lastBatchSent = Instant.now();
            messagesSinceLastBatchSent = 0;
        }

        final var numMessages = getNumLogLines(logs);
        LOG.info("Successfully sent batch of {} messages to loki", numMessages);

        logs.clear();
    }



    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.err.println("Requires arguments: <path/to/config.properties>");
            System.exit(1);
        }

        final var config = new TwitchChatLokiClientConfig(Paths.get(args[0]));
        new TwitchChatLokiClient(config);
        // Client will continue to run in background
    }

    private static int getNumLogLines(Map<LogStream, List<LogEntry>> batch) {
        return batch.values().stream().mapToInt(List::size).sum();
    }

    private static LogStream getStreamForTwitchChannel(EventChannel channel) {
        return new LogStream(Map.of("channel", channel.getName()));
    }
}
