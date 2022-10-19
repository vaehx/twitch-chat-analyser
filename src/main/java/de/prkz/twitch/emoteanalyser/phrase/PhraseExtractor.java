package de.prkz.twitch.emoteanalyser.phrase;

import de.prkz.twitch.emoteanalyser.Message;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class PhraseExtractor extends ProcessFunction<Message, PhraseStats> {

    private static final Logger LOG = LoggerFactory.getLogger(PhraseExtractor.class);

    private static final String PHRASES_TABLE = "phrases";
    private static final Long PHRASE_RELOAD_INTERVAL_MILLIS = 60 * 1000L;

    private transient long lastPhraseReload;
    private transient Set<Phrase> phrases;
    private transient Lock reloadLock;

    private final String jdbcUrl;
    private final OutputTag<MessageMatchingPhrase> matchedMessagesOutputTag;

    public PhraseExtractor(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
        this.matchedMessagesOutputTag = new OutputTag<MessageMatchingPhrase>("messagesMatchingPhrase") {};
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        phrases = new HashSet<>();
        reloadLock = new ReentrantLock();

        // No need to lock in open()
        lastPhraseReload = 0; // forces reload
        reloadPhrasesIfNecessary();
    }

    public OutputTag<MessageMatchingPhrase> getMatchedMessagesOutputTag() {
        return matchedMessagesOutputTag;
    }

    @Override
    public void processElement(Message message, Context ctx, Collector<PhraseStats> out) throws Exception {
        // We have to lock this hole block since reloading may otherwise take place while we match the message
        reloadLock.lock();
        try {
            reloadPhrasesIfNecessary();

            // There may be multiple phrases/regexes matching within a single message!
            for (Phrase phrase : phrases) {
                if (phrase.channelPattern != null) {
                    Matcher channelMatcher = phrase.channelPattern.matcher(message.channel);
                    if (!channelMatcher.matches())
                        continue;
                }

                Matcher matcher = phrase.pattern.matcher(message.message);
                int matches = 0;
                while (matcher.find()) {
                    matches++;
                }

                if (matches > 0) {
                    PhraseStats p = new PhraseStats();
                    p.instant = message.instant;
                    p.channel = message.channel;
                    p.phraseName = phrase.name;
                    p.matches = matches;
                    out.collect(p);

                    if (phrase.logMessage) {
                        // Even if the phrase matched multiple times, only log it once per phrase
                        ctx.output(matchedMessagesOutputTag, new MessageMatchingPhrase(message, phrase));
                    }
                }
            }
        } finally {
            reloadLock.unlock();
        }
    }

    private void reloadPhrasesIfNecessary() throws Exception {
        long startTime = System.currentTimeMillis();

        if (startTime - lastPhraseReload < PHRASE_RELOAD_INTERVAL_MILLIS)
            return; // no need to reload yet

        Class.forName(org.postgresql.Driver.class.getCanonicalName());
        try (Connection conn = DriverManager.getConnection(jdbcUrl); Statement stmt = conn.createStatement()) {
            ResultSet resultSet = stmt.executeQuery(
                    "SELECT name, regex, channel_filter_regex, log_message FROM " + PHRASES_TABLE);

            phrases.clear();
            while (resultSet.next()) {
                String name = resultSet.getString("name");

                String regex = resultSet.getString("regex");
                Pattern pattern;
                try {
                    pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
                } catch (PatternSyntaxException e) {
                    LOG.error("Invalid regex '{}' for phrase '{}'. Will ignore this phrase!", regex, name);
                    continue;
                }

                String channelFilterRegex = resultSet.getString("channel_filter_regex");
                Pattern channelPattern;
                try {
                    channelPattern = !resultSet.wasNull()
                        ? Pattern.compile(channelFilterRegex, Pattern.CASE_INSENSITIVE)
                        : null;
                } catch (PatternSyntaxException e) {
                    LOG.error("Invalid channel filter regex: '{}' for phrase '{}'. Will ignore this phrase!",
                            channelFilterRegex, name);
                    continue;
                }

                Phrase phrase = new Phrase(
                        resultSet.getString("name"),
                        pattern,
                        channelPattern,
                        resultSet.getBoolean("log_message"));
                phrases.add(phrase);
            }
        } catch (SQLException e) {
            throw new Exception("Could not reload phrases because of an SQL error", e);
        }

        long now = System.currentTimeMillis();
        lastPhraseReload = now;

        LOG.debug("Reloaded {} phrases in {} ms", phrases.size(), now - startTime);
    }

    public static void prepareTables(Statement stmt) throws SQLException {
        stmt.execute("CREATE TABLE IF NOT EXISTS " + PHRASES_TABLE + "(" +
                "name VARCHAR(64) NOT NULL," +
                "regex VARCHAR NOT NULL," +
                "channel_filter_regex VARCHAR DEFAULT NULL," +
                "log_message BOOLEAN NOT NULL DEFAULT false," +
                "PRIMARY KEY(name))");
    }
}
