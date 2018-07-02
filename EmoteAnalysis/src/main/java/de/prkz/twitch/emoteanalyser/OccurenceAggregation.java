package de.prkz.twitch.emoteanalyser;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Key: (username, emote)
public class OccurenceAggregation
		extends ProcessWindowFunction<Emote, EmoteOccurences, Tuple2<String, String>, TimeWindow> {

	private static final Logger LOG = LoggerFactory.getLogger(OccurenceAggregation.class);

	private transient ValueState<EmoteOccurences> occurencesState;

	@Override
	public void open(Configuration parameters) throws Exception {
		occurencesState = getRuntimeContext().getState(new ValueStateDescriptor<>(
				"occurences", TypeInformation.of(new TypeHint<EmoteOccurences>() {})));
	}

	@Override
	public void process(Tuple2<String, String> key,
						Context context,
						Iterable<Emote> emotes,
						Collector<EmoteOccurences> collector) throws Exception {

		LOG.info("OccurenceAggregation::process()");

		EmoteOccurences occurences = occurencesState.value();
		if (occurences == null) {

			// TODO: Load current count from database, if it exists

			occurences = new EmoteOccurences();
			occurences.username = key.f0;
			occurences.emote = key.f1;
			occurences.occurrences = 0;
		}

		// Increase occurrence count
		for (Emote e : emotes)
			occurences.occurrences++;

		// Emit current occurrence count
		occurences.timestamp = context.window().getEnd();
		collector.collect(occurences);

		LOG.info("user: " + occurences.username + ", emote: " + occurences.emote + ", occurences: " + occurences.occurrences);

		occurencesState.update(occurences);
	}
}
