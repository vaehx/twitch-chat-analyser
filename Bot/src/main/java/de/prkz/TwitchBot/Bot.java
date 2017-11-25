package de.prkz.TwitchBot;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.pircbotx.hooks.ListenerAdapter;
import org.pircbotx.hooks.events.MessageEvent;

public class Bot extends ListenerAdapter {

	protected KafkaProducer<String, String> producer;

	public Bot(KafkaProducer<String, String> producer) {
		this.producer = producer;
	}

	@Override
	public void onMessage(MessageEvent event) throws Exception {
		producer.send(new ProducerRecord<>("twitch-chat", event.getUser().getNick(), event.getMessage()));
	}
}
