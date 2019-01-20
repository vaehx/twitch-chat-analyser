package de.prkz.twitch.emoteanalyser;

import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class MessageSerializer implements Serializer<Message> {

    @Override
    public byte[] serialize(String s, Message message) {
        return message.toJson().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void configure(Map<String, ?> map, boolean b) {
    }

    @Override
    public void close() {
    }
}
