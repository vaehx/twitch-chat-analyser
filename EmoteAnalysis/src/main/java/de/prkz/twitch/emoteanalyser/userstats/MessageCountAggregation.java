package de.prkz.twitch.emoteanalyser.userstats;

import de.prkz.twitch.emoteanalyser.Message;
import de.prkz.twitch.emoteanalyser.emotes.OccurenceAggregation;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class MessageCountAggregation
		extends ProcessWindowFunction<Message, UserStats, String, TimeWindow> {

	private static final Logger LOG = LoggerFactory.getLogger(OccurenceAggregation.class);

	private transient ValueState<UserStats> userStatsState;
	private transient Connection conn;
	private String jdbcUrl;

	public MessageCountAggregation(String jdbcUrl) {
		this.jdbcUrl = jdbcUrl;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		userStatsState = getRuntimeContext().getState(new ValueStateDescriptor<>(
				"userStats", TypeInformation.of(new TypeHint<UserStats>() {})));

		conn = DriverManager.getConnection(jdbcUrl);
	}

	@Override
	public void process(String username,
						Context context,
						Iterable<Message> messages,
						Collector<UserStats> collector) throws Exception {

		UserStats stats = userStatsState.value();
		if (stats == null) {
			stats = new UserStats();
			stats.username = username;

			// Load previous counts from db, if any
			Statement stmt = conn.createStatement();
			ResultSet result = stmt.executeQuery("SELECT message_count FROM users WHERE username='" + username + "' " +
					"ORDER BY timestamp DESC LIMIT 1");

			if (result.next())
				stats.messageCount = result.getLong(1);
			else
				stats.messageCount = 0;

			stmt.close();
		}

		// Increase message count
		for (Message m : messages)
			stats.messageCount++;

		// Emit current count
		stats.timestamp = context.window().getEnd();
		collector.collect(stats);

		LOG.info("user: " + username + ", messages: " + stats.messageCount);

		userStatsState.update(stats);
	}

	@Override
	public void close() throws Exception {
		conn.close();
	}
}
