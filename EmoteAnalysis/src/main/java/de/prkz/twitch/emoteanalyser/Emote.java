package de.prkz.twitch.emoteanalyser;

public class Emote {
	public long timestamp;
	public String username;
	public String emote;

	public static boolean isEmote(String emote) {
		String[] emotes = new String[] {
				"moon2MLEM",
				"moon2N",
				"moon2S",
				"moon2A",
				"moon2C",
				"moon2SMUG",
				"moon2E",
				"moon2L",
				"moon2WINKY",
				"moon2CUTE",
				"KKona",
				"DansGame",
				"PogChamp",
				"LuL",
				"LUL",
				":)",
				"pepeD",
				"moon2O",
				"CoolCat"
		};

		for (String validEmote : emotes) {
			if (emote.compareTo(validEmote) == 0)
				return true;
		}

		return false;
	}
}
