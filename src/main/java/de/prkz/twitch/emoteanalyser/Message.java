package de.prkz.twitch.emoteanalyser;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

public class Message {
	public long timestamp;
	public String channel;
	public String username;
	public String message;

	public static class UsernameKeySelector implements KeySelector<Message, String> {
		@Override
		public String getKey(Message message) throws Exception {
			return message.username;
		}
	}

	public static class TimestampExtractor extends AscendingTimestampExtractor<Message> {
		@Override
		public long extractAscendingTimestamp(Message message) {
			return message.timestamp;
		}
	}
}
