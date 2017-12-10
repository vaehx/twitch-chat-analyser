package de.prkz.TwitchBot;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class KafkaJsonSerializer<T> implements Serializer<T> {

	private ObjectMapper objectMapper;

	public KafkaJsonSerializer() {
	}

	@Override
	public void configure(Map<String, ?> map, boolean b) {
		objectMapper = new ObjectMapper();
	}

	@Override
	public byte[] serialize(String topic, T data) {
		if (data == null)
			return null;

		try {
			return objectMapper.writeValueAsBytes(data);
		} catch (Exception e) {
			throw new SerializationException("Error serializing JSON message", e);
		}
	}

	@Override
	public void close() {
	}
}
