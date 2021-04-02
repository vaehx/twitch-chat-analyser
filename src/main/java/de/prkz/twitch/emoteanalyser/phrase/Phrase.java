package de.prkz.twitch.emoteanalyser.phrase;

import java.io.Serializable;
import java.util.regex.Pattern;

public class Phrase implements Serializable {
    final String name;
    final Pattern pattern;

    /** Optional, may be null to match all channels */
    final Pattern channelPattern;

    final boolean logMessage;


    /**
     * @param channelPattern Optional, may be null to match all channels
     */
    public Phrase(String name, Pattern pattern, Pattern channelPattern, boolean logMessage) {
        this.name = name;
        this.pattern = pattern;
        this.channelPattern = channelPattern;
        this.logMessage = logMessage;
    }
}
