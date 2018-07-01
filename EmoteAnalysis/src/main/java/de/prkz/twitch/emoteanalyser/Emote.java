package de.prkz.twitch.emoteanalyser;

public class Emote {
	public String username;
	public String emote;

	public static boolean isEmote(String emote) {
		String[] emotes = new String[] {
				"moon2MLEM", "moon2S", "moon2A", "PogChamp", "LuL"
		};

		for (String validEmote : emotes) {
			if (emote.compareTo(validEmote) == 0)
				return true;
		}

		return false;
	}
}
