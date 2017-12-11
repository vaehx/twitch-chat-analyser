package de.prkz.TwitchBot;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.pircbotx.User;
import org.pircbotx.hooks.ListenerAdapter;
import org.pircbotx.hooks.events.MessageEvent;

import java.sql.*;
import java.util.Properties;

public class Bot extends ListenerAdapter {

	protected KafkaProducer<Long, Message> producer;

	protected Connection jdbcConn;
	protected PreparedStatement stmt;
	protected int numBatches = 0;

	public Bot(String kafkaBootstrapServers) throws Exception {
		// Setup kafka producer
		Properties producerProps = new Properties();
		producerProps.put("bootstrap.servers", kafkaBootstrapServers);
		producerProps.put("key.serializer", LongSerializer.class);
		producerProps.put("value.serializer", KafkaJsonSerializer.class);

//		producer = new KafkaProducer<>(producerProps);

		// Setup jdbc connection
		jdbcConn = DriverManager.getConnection("jdbc:mysql://localhost:3306/twitch", "root", "");
		jdbcConn.setAutoCommit(false);
	}

	@Override
	public void onMessage(MessageEvent event) throws Exception {
		User user = event.getUser();
		if (user == null)
			return;

		try {
			if (stmt == null) {
				String sql = "INSERT INTO message(timestamp, channel, user, content) VALUES(?, ?, ?, ?)";
				stmt = jdbcConn.prepareStatement(sql);
				numBatches = 0;
			}

			stmt.setLong(1, event.getTimestamp());
			stmt.setString(2, event.getChannel().getName());
			stmt.setString(3, event.getUser().getNick());
			stmt.setString(4, event.getMessage());
			stmt.addBatch();

			numBatches++;

			if (numBatches >= 20) {
				long tsBefore = System.currentTimeMillis();
				stmt.executeBatch();
				stmt = null;
				jdbcConn.commit();
				long duration = System.currentTimeMillis() - tsBefore;
				System.out.println("Executed batch of " + numBatches + " inserts. Executed and committed in " + duration + "ms");
			}
		}
		catch (Exception ex) {
			System.err.println("Error: " + ex.getMessage());
			ex.printStackTrace();
		}

		/*Message message = new Message(
				event.getChannel().getName(),
				user.getNick(),
				event.getMessage());

		producer.send(new ProducerRecord<>("twitch-chat", event.getTimestamp(), message));*/
	}
}
