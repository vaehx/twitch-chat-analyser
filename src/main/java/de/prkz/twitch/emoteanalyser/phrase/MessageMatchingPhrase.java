package de.prkz.twitch.emoteanalyser.phrase;

import de.prkz.twitch.emoteanalyser.Message;

public class MessageMatchingPhrase extends Message {
    public Phrase phrase;

    public MessageMatchingPhrase(Message message, Phrase phrase) {
        super(message);
        this.phrase = phrase;
    }

    // Empty ctor for flink serializer
    public MessageMatchingPhrase() {
    }
}
