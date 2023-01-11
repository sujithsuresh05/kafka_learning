package com.kafka.streams;

import java.util.Optional;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import com.google.gson.JsonParser;

public class StreamFilterTweets {

	private static final String BOOT_STRAP_SERVER = "localhost:9092";
	private static final String APPLICATION_GROUP_ID = "demo-kafka-streams";
	private static final String TOPIC = "twitter_topic_kafka";
	private static final String IMPORTANT_TWEET_TOPIC = "important-tweets";
	private static final JsonParser JSON_PARSER = new JsonParser();

	public static void main(String args[]) {

		// create properties
		Properties properties = new Properties();
		properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOT_STRAP_SERVER);
		properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_GROUP_ID);
		properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
		properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

		// create a topology
		StreamsBuilder streamsBuilder = new StreamsBuilder();

		// input logic
		KStream<String, String> inputTopic = streamsBuilder.stream(TOPIC);

		KStream<String, String> filteredStream = inputTopic.filter((k, jsonTweet) -> {
			// filter stream it have at least one like
			Integer likeCount = extractLikeCount(jsonTweet);
			return !Optional.of(likeCount).isPresent() ? false : (likeCount > 0 ? true : false);
		});

		filteredStream.to(IMPORTANT_TWEET_TOPIC);
		// build the topology
		KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);
		kafkaStreams.start();

	}

	private static Integer extractLikeCount(String jsonTweet) {
		// gson libaray
		return JSON_PARSER.parse(jsonTweet).getAsJsonObject().get("likeCount").getAsInt();
	}
}
