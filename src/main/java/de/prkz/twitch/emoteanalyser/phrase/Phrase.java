package de.prkz.twitch.emoteanalyser.phrase;

import java.io.Serializable;
import java.util.regex.Pattern;

public class Phrase implements Serializable {
    final String name;
    final Pattern pattern;
    final boolean logMessage;

    public Phrase(String name, Pattern pattern, boolean logMessage) {
        this.name = name;
        this.pattern = pattern;
        this.logMessage = logMessage;
    }
}
