package de.prkz.twitch.emoteanalyser.emote;

public enum EmoteType {
    TWITCH_SUBSCRIBER((short)0),
    TWITCH_GLOBAL((short)1),
    BTTV((short)2),
    FFZ((short)3),
    EMOJI((short)4),
    SEVENTV((short)5);

    private final short identifier;

    EmoteType(short identifier) {
        this.identifier = identifier;
    }

    public short identifier() {
        return identifier;
    }
}
