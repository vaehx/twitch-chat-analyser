package de.prkz.twitch.emoteanalyser;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class MessageDeserializationSchema implements DeserializationSchema<Message> {
	@Override
	public Message deserialize(byte[] bytes) throws IOException {
		return Message.fromJson(new String(bytes));
	}

	@Override
	public boolean isEndOfStream(Message message) {
		return false;
	}

	@Override
	public TypeInformation<Message> getProducedType() {
		return TypeInformation.of(new TypeHint<Message>() {});
	}
}
