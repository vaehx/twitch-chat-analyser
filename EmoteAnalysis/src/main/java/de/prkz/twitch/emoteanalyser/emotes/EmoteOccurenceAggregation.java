package de.prkz.twitch.emoteanalyser.emotes;

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

// Key: emote
public class EmoteOccurenceAggregation
		extends ProcessWindowFunction<Emote, EmoteOccurences, String, TimeWindow> {

	private static final Logger LOG = LoggerFactory.getLogger(EmoteOccurenceAggregation.class);

	private transient ValueState<EmoteOccurences> occurencesState;
	private transient Connection conn;
	private String jdbcUrl;

	public EmoteOccurenceAggregation(String jdbcUrl) {
		this.jdbcUrl = jdbcUrl;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		occurencesState = getRuntimeContext().getState(new ValueStateDescriptor<>(
				"occurences", TypeInformation.of(new TypeHint<EmoteOccurences>() {})));

		conn = DriverManager.getConnection(jdbcUrl);
	}

	@Override
	public void process(String key,
						Context context,
						Iterable<Emote> emotes,
						Collector<EmoteOccurences> collector) throws Exception {

		EmoteOccurences occurences = occurencesState.value();
		if (occurences == null) {
			occurences = new UserEmoteOccurences();
			occurences.emote = key;

			// Load current count from database, if it exists
			Statement stmt = conn.createStatement();
			ResultSet result = stmt.executeQuery("SELECT occurrences FROM emote_totals " +
					"WHERE emote='" + occurences.emote + "' " +
					"ORDER BY timestamp DESC LIMIT 1");

			if (result.next())
				occurences.occurrences = result.getLong(1);
			else
				occurences.occurrences = 0;

			stmt.close();
		}

		// Increase occurrence count
		for (Emote e : emotes)
			occurences.occurrences++;

		// Emit current occurrence count
		occurences.timestamp = context.window().getEnd();
		collector.collect(occurences);

		//LOG.info("emote: " + occurences.emote + ", occurrences: " + occurences.occurrences);

		occurencesState.update(occurences);
	}

	@Override
	public void close() throws Exception {
		conn.close();
	}
}
