package de.prkz.twitch.emoteanalyser.emotes;

public class Emote {
	public long timestamp;
	public String username;
	public String emote;

	public static boolean isEmote(String emote) {
		String[] emotes = new String[] {
			/* moonmoon_ow */
			"moon2MLADY", "moon2WINKY", "moon2TEEHEE", "moon2GOOD", "moon2DUMB", "moon2GUMS",
			"moon2MLEM", "moon2FEELS", "moon2KISSES", "moon2BANNED", "moon2SHRUG", "moon21",
			"moon22", "moon23", "moon24", "moon2CREEP", "moon2YE", "moon2GASM", "moon2SMAG",
			"moon2COFFEE", "moon2PLSNO", "moon2HEY", "moon2XD", "moon2T", "moon2L", "moon2SPY",
			"moon2W", "moon2P", "moon2WUT", "moon2WOW", "moon2NOM", "moon2WAH", "moon2E",
			"moon2LUL", "moon2EZ", "moon2S", "moon2CUTE", "moon2M", "moon2MM", "moon2LURK",
			"moon2C", "moon2D", "moon2A", "moon2OWO", "moon2MD", "moon2SMUG", "moon2H",
			"moon2CD", "moon2N", "moon2O",

			/* lirik */
			"lirikN", "lirikNO", "lirikWINK", "lirikD", "lirikTOS",

			/* forsen */
			"forsenE", "forsenW", "forsenKek", "forsenRedSonic", "forsen1",

			/* naro */
			"naroRage", "naroXD", "naroStaryn", "naroWhat", "naroGasm", "naroSmug",
			"naroThug", "naroSad", "naroEh", "naroGold", "naroWink", "naroNyrats",

			/* nightblue */
			"nb3W",

			/* Global Twitch emotes */
			"CoolCat", "KKona", "DansGame", "PogChamp", "LUL", "TriHard", "Kappa", "KappaPride",
			"MingLee", "KreyGasm", "Jebaited", "VoHiYo", "TPFufun",

			/* BTTV */
			"LuL", "pepeD", "FeelsGoodMan", "VapeNation", "FeelsBirthdayMan", "D:", "NaM",
			"PepePls", "lulWut", "FeelsWeirdMan", "monkaS", "monkaMEGA", "monkaSHAKE",
			"pepoCheer", "Clap", "Clap2", "HYPERCLAP", "HYPEROMEGAPOGGERS", "gachiBASS", "ZOINKS",
			"ppHop", "HYPERLUL", "TriKool", "CiGrip", "pepeBASS",

			/* FFZ */
			"AYAYA",

			/* Emojis */
			":)",
		};

		for (String validEmote : emotes) {
			if (emote.compareTo(validEmote) == 0)
				return true;
		}

		return false;
	}
}
