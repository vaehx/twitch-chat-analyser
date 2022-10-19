package de.prkz.twitch.emoteanalyser.emote;

import java.time.Instant;

public class Emote {

    /**
     * Max sensible allowed number of characters in an emote name/string. Any emote longer than that will cause a
     * failure.
     */
    public static final int MAX_EMOTE_LENGTH = 150;

    public Instant instant;
    public String channel;
    public String username;
    public String emote;
}
