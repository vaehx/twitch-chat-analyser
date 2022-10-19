package de.prkz.twitch.emoteanalyser;

import java.io.Serializable;
import java.time.Instant;

public abstract class AbstractStats implements Serializable {
    public Instant instant;
}
