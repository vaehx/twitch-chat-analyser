package de.prkz.twitch.emoteanalyser;

import org.apache.flink.api.java.functions.KeySelector;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.Serializable;
import java.time.Instant;

public class Message implements Serializable {
    public Instant instant;
    public String channel;
    public String username;
    public String message;

    public Message() {
    }

    public Message(Message o) {
        instant = o.instant;
        channel = o.channel;
        username = o.username;
        message = o.message;
    }

    public String toJson() {
        JSONObject obj = new JSONObject();
        obj.put("timestamp", instant.toEpochMilli());
        obj.put("channel", channel);
        obj.put("username", username);
        obj.put("message", message);
        return obj.toString();
    }

    public static Message fromJson(String json) throws JSONException {
        JSONObject obj = new JSONObject(json);
        Message m = new Message();
        m.instant = Instant.ofEpochMilli(obj.getLong("timestamp"));
        m.channel = obj.getString("channel");
        m.username = obj.getString("username");
        m.message = obj.getString("message");
        return m;
    }

    public static class UsernameKeySelector implements KeySelector<Message, String> {
        @Override
        public String getKey(Message message) throws Exception {
            return message.username;
        }
    }
}
