package de.prkz.twitch.emoteanalyser;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.json.JSONException;
import org.json.JSONObject;

public class Message {
    public long timestamp;
    public String channel;
    public String username;
    public String message;

    public String toJson() {
        JSONObject obj = new JSONObject();
        obj.put("timestamp", timestamp);
        obj.put("channel", channel);
        obj.put("username", username);
        obj.put("message", message);
        return obj.toString();
    }

    public static Message fromJson(String json) throws JSONException {
        JSONObject obj = new JSONObject(json);
        Message m = new Message();
        m.timestamp = obj.getLong("timestamp");
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

    public static class TimestampExtractor extends BoundedOutOfOrdernessTimestampExtractor<Message> {

        public TimestampExtractor(long maxOutOfOrdernessMs) {
            super(Time.milliseconds(maxOutOfOrdernessMs));
        }

        @Override
        public long extractTimestamp(Message message) {
            return message.timestamp;
        }
    }
}
