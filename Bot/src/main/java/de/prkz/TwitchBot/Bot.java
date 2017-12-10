package de.prkz.TwitchBot;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.pircbotx.User;
import org.pircbotx.hooks.ListenerAdapter;
import org.pircbotx.hooks.events.MessageEvent;

import java.util.Properties;

public class Bot extends ListenerAdapter {

	protected KafkaProducer<Long, Message> producer;

	public Bot(String kafkaBootstrapServers) {
		// Setup kafka producer
		Properties producerProps = new Properties();
		producerProps.put("bootstrap.servers", kafkaBootstrapServers);
		producerProps.put("key.serializer", LongSerializer.class);
		producerProps.put("value.serializer", KafkaJsonSerializer.class);

		producer = new KafkaProducer<>(producerProps);
	}

	@Override
	public void onMessage(MessageEvent event) throws Exception {
		User user = event.getUser();
		if (user == null)
			return;

		Message message = new Message(
				event.getChannel().getName(),
				user.getNick(),
				event.getMessage());

		producer.send(new ProducerRecord<>("twitch-chat", event.getTimestamp(), message));
	}
}
