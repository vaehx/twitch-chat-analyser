package de.prkz.twitch.emoteanalyser;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmoteExtractor implements FlatMapFunction<Message, Emote> {

	private static final Logger LOG = LoggerFactory.getLogger(EmoteExtractor.class);

	@Override
	public void flatMap(Message message, Collector<Emote> collector) throws Exception {
		String[] words = message.message.split("\\s+");
		for (String word : words) {
			if (Emote.isEmote(word)) {
				Emote e = new Emote();
				e.emote = word;
				e.username = message.username;
				collector.collect(e);
			}
		}
	}
}
