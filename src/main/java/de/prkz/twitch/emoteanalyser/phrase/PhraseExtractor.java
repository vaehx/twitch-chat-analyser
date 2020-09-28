package de.prkz.twitch.emoteanalyser.phrase;

import de.prkz.twitch.emoteanalyser.Message;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PhraseExtractor extends RichFlatMapFunction<Message, PhraseStats> {

    private static final Logger LOG = LoggerFactory.getLogger(PhraseExtractor.class);

    private static final String PHRASES_TABLE = "phrases";
    private static final Long PHRASE_RELOAD_INTERVAL_MILLIS = 60 * 1000L;

    private transient long lastPhraseReload;
    private transient Set<Phrase> phrases;
    private transient Lock reloadLock;

    private final String jdbcUrl;

    public PhraseExtractor(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        phrases = new HashSet<>();
        reloadLock = new ReentrantLock();

        // No need to lock in open()
        lastPhraseReload = 0; // forces reload
        reloadPhrasesIfNecessary();
    }

    @Override
    public void flatMap(Message message, Collector<PhraseStats> out) throws Exception {
        // We have to lock this hole block since reloading may otherwise take place while we match the message
        reloadLock.lock();
        try {
            reloadPhrasesIfNecessary();

            // There may be multiple phrases/regexes matching the message
            for (Phrase phrase : phrases) {
                Matcher matcher = phrase.pattern.matcher(message.message);

                int matches = 0;
                while (matcher.find()) {
                    matches++;
                }

                if (matches > 0) {
                    PhraseStats p = new PhraseStats();
                    p.timestamp = message.timestamp;
                    p.channel = message.channel;
                    p.phraseName = phrase.name;
                    p.matches = matches;
                    out.collect(p);
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
            ResultSet resultSet = stmt.executeQuery("SELECT name, regex FROM " + PHRASES_TABLE);

            phrases.clear();
            while (resultSet.next()) {
                Pattern pattern = Pattern.compile(resultSet.getString("regex"), Pattern.CASE_INSENSITIVE);
                phrases.add(new Phrase(resultSet.getString("name"), pattern));
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
                "PRIMARY KEY(name))");
    }


    private static class Phrase {
        final String name;
        final Pattern pattern;

        public Phrase(String name, Pattern pattern) {
            this.name = name;
            this.pattern = pattern;
        }
    }
}
