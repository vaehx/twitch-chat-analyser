package de.prkz.twitch.emoteanalyser.bot;

import de.prkz.twitch.emoteanalyser.Message;
import de.prkz.twitch.emoteanalyser.MessageSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.pircbotx.PircBotX;
import org.pircbotx.hooks.ListenerAdapter;
import org.pircbotx.hooks.events.MessageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Bot extends ListenerAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(Bot.class);

    private static final String TOPIC = "TwitchMessages";

    private KafkaProducer<Long, Message> producer;

    public static void main(String args[]) throws Exception {
        if (args.length <= 1) {
            System.err.println("Requires arguments: <kafka-bootstrap-server> <channel...>");
            System.exit(1);
        }

        String kafkaBootstrapServer = args[0];
        List<String> channels = new ArrayList<>();
        for (int i = 1; i < args.length; ++i) {
            LOG.info("Will join channel " + args[i]);
            channels.add("#" + args[i]);
        }

        LOG.info("Connecting to Kafka cluster...");
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", kafkaBootstrapServer);
        kafkaProps.put("key.serializer", LongSerializer.class);
        kafkaProps.put("value.serializer", MessageSerializer.class);
        kafkaProps.put("linger.ms", 100);

        Bot bot = new Bot();
        bot.producer = new KafkaProducer<>(kafkaProps);

        org.pircbotx.Configuration config = new org.pircbotx.Configuration.Builder()
                .setName("justinfan92834") // TODO: Make random?
                .addServer("irc.chat.twitch.tv", 6667)
                .addListener(bot)
                .addAutoJoinChannels(channels)
                .setAutoReconnect(true)
                .buildConfiguration();

        LOG.info("Now starting bot...");
        PircBotX pircBotX = new PircBotX(config);
        pircBotX.startBot();
    }

    @Override
    public void onMessage(MessageEvent event) throws Exception {
        if (event.getUser() == null)
            return;

        Message m = new Message();
        m.channel = event.getChannel().getName();
        if (m.channel.startsWith("#"))
            m.channel = m.channel.substring(1);

        m.timestamp = event.getTimestamp();
        m.username = event.getUser().getNick();
        m.message = event.getMessage();

        producer.send(new ProducerRecord<>(TOPIC, m.timestamp, m));
    }
}
