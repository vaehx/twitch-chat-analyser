package de.prkz.twitch.emoteanalyser;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.pircbotx.PircBotX;
import org.pircbotx.hooks.ListenerAdapter;
import org.pircbotx.hooks.events.MessageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TwitchSource extends RichSourceFunction<Message> {

	private static final Logger LOG = LoggerFactory.getLogger(TwitchSource.class);

	private transient PircBotX bot;
	private transient final Lock lock = new ReentrantLock();
	private transient final Condition cancelled = lock.newCondition();

	private String[] channels;

	/**
	 * @param channels must start with '#'
	 */
	public TwitchSource(String[] channels) {
		this.channels = channels;
	}

	@Override
	public void run(SourceContext<Message> sourceContext) throws Exception {

		for (String channel : channels)
			LOG.info("Will join channel " + channel);

		org.pircbotx.Configuration config = new org.pircbotx.Configuration.Builder()
				.setName("justinfan92834") // TODO: Make random?
				.addServer("irc.chat.twitch.tv", 6667)
				.addListener(new Bot(sourceContext))
				.addAutoJoinChannels(Arrays.asList(channels))
				.setAutoReconnect(true)
				.buildConfiguration();
		bot = new PircBotX(config);
		bot.startBot();

		cancelled.await();
		bot.stopBotReconnect();
	}

	@Override
	public void cancel() {
		cancelled.signal();
	}

	@Override
	public void close() throws Exception {
		super.close();
		if (bot != null) {
			bot.stopBotReconnect();
			bot.close();
		}
	}



	public class Bot extends ListenerAdapter {

		private SourceContext<Message> ctx;

		public Bot(SourceContext<Message> ctx) {
			this.ctx = ctx;
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

			//LOG.info(m.username + ": " + m.message);

			synchronized (ctx.getCheckpointLock()) {
				ctx.collectWithTimestamp(m, m.timestamp);
			}
		}
	}
}
