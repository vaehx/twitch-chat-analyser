package de.prkz.TwitchBot;

public class Message {

	String channel;
	String username;
	String text;

	public Message(String channel, String username, String text) {
		this.channel = channel;
		this.username = username;
		this.text = text;
	}

	public String getChannel() {
		return channel;
	}

	public String getUsername() {
		return username;
	}

	public String getText() {
		return text;
	}
}
