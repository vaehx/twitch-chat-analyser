package de.prkz.TwitchBot;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.pircbotx.Configuration;
import org.pircbotx.PircBotX;

public class Main {

	static Logger logger = LogManager.getLogger(Main.class);

	public static PircBotX bot;

	public static void main(String[] args) throws Exception {

		// Setup twitch IRC bot
		Configuration config = new Configuration.Builder()
				.setName("justinfan618723")
				.addServer("irc.chat.twitch.tv", 6667)
				.addListener(new Bot("docker-host:9092"))
				.addAutoJoinChannel("#lirik")
				.buildConfiguration();

		bot = new PircBotX(config);
		bot.startBot();
	}
}
