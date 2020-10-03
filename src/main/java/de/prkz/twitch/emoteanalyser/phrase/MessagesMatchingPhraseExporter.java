package de.prkz.twitch.emoteanalyser.phrase;

import de.prkz.twitch.emoteanalyser.XAPostgresSink;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Calendar;
import java.util.TimeZone;

public class MessagesMatchingPhraseExporter extends XAPostgresSink<MessageMatchingPhrase> {

    private static final String MESSAGES_MATCHING_PHRASE_TABLE = "messages_matching_phrase";


    public MessagesMatchingPhraseExporter(String jdbcUrl) {
        super(jdbcUrl, 0);
    }

    @Override
    protected String getInsertSQL() {
        return "INSERT INTO " + MESSAGES_MATCHING_PHRASE_TABLE + "" +
                "(message_time, message_user, message_text, matched_phrase) " +
                "VALUES(?, ?, ?, ?)";
    }

    @Override
    protected void setFields(PreparedStatement stmt, MessageMatchingPhrase messageMatchingPhrase) throws SQLException {
        stmt.setTimestamp(1, Timestamp.from(Instant.ofEpochMilli(messageMatchingPhrase.timestamp)),
                Calendar.getInstance(TimeZone.getTimeZone("UTC")));
        stmt.setString(2, messageMatchingPhrase.username);
        stmt.setString(3, messageMatchingPhrase.message);
        stmt.setString(4, messageMatchingPhrase.phrase.name);
    }

    public static void prepareTables(Statement stmt) throws SQLException {
        stmt.execute("CREATE TABLE IF NOT EXISTS " + MESSAGES_MATCHING_PHRASE_TABLE + "(" +
                "message_time TIMESTAMPTZ," +
                "message_user VARCHAR NOT NULL," +
                "message_text VARCHAR NOT NULL," +
                "matched_phrase VARCHAR NOT NULL)");
    }
}
