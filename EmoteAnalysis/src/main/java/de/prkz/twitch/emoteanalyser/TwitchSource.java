package de.prkz.twitch.emoteanalyser;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.pircbotx.PircBotX;
import org.pircbotx.hooks.ListenerAdapter;
import org.pircbotx.hooks.events.MessageEvent;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TwitchSource extends RichSourceFunction<Message> {

	private transient PircBotX bot;
	private transient final Lock lock = new ReentrantLock();
	private transient final Condition cancelled = lock.newCondition();

	private String channel;

	public TwitchSource(String channel) {
		this.channel = channel;
	}

	@Override
	public void run(SourceContext<Message> sourceContext) throws Exception {
		org.pircbotx.Configuration config = new org.pircbotx.Configuration.Builder()
				.setName("justinfan92834")
				.addServer("irc.chat.twitch.tv", 6667)
				.addListener(new Bot(sourceContext))
				.addAutoJoinChannel("#" + channel)
				.buildConfiguration();
		bot = new PircBotX(config);
		bot.startBot();

		cancelled.await();
	}

	@Override
	public void cancel() {
		cancelled.signal();
	}

	@Override
	public void close() throws Exception {
		super.close();
		if (bot != null)
			bot.close();
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
			m.timestamp = event.getTimestamp();
			m.username = event.getUser().getNick();
			m.message = event.getMessage();

			synchronized (ctx.getCheckpointLock()) {
				ctx.collectWithTimestamp(m, m.timestamp);
			}
		}
	}
}
