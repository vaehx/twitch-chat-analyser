package de.prkz.twitch.emoteanalyser.emote;

import de.prkz.twitch.emoteanalyser.AbstractStats;

/**
 * Per-Emote statistics entry
 */
public class EmoteStats extends AbstractStats {
    public String channel;
    public String emote;
    public long totalOccurrences; // aggregated over global event-time window
    public int occurrences = 0; // aggregated over tumbling event-time window
}
