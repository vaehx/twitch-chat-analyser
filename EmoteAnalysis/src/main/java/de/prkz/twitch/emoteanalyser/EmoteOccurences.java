package de.prkz.twitch.emoteanalyser;

/**
 * Describes a sample of the number occurrences of an emote from a user at a timestamp
 */
public class EmoteOccurences {
	public long timestamp;
	public String username;
	public String emote;
	public long occurrences;
}
