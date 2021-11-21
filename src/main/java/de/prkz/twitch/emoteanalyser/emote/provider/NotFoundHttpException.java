package de.prkz.twitch.emoteanalyser.emote.provider;

public class NotFoundHttpException extends Exception {

    public NotFoundHttpException(String message) {
        super(message);
    }

    public NotFoundHttpException() {
        super();
    }
}
