package de.prkz.twitch.emoteanalyser.user;

import de.prkz.twitch.emoteanalyser.AbstractStats;

public class UserStats extends AbstractStats {
	public String channel;
	public String username;
	public long totalMessageCount; // aggregated over global window
	public int messageCount = 0; // aggregated over tumbling event-time window
}
