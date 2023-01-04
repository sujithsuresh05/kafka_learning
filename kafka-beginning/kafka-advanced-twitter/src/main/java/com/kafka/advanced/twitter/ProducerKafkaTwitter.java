package com.kafka.advanced.twitter;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.redouane59.twitter.dto.tweet.Tweet;
import com.kafka.advanced.tweet.dto.TweetData;

public class ProducerKafkaTwitter {
	private static Logger logger = LoggerFactory.getLogger(ProducerKafkaTwitter.class.getName());
	private static final String TWITTER_TOPIC = "twitter_topic_kafka";

	public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
			.setSerializationInclusion(JsonInclude.Include.NON_NULL);
	public static void main(String args[]) {
		new ProducerKafkaTwitter().run();
	}

	public void run() {
		try {
			// Create tweet fetcher client
			TweetFetcher twiterClientFetcher = new TweetFetcher("D:/twitter_keys.json");
			List<Tweet> tweets = twiterClientFetcher.searchForTweets("kafka");

			// create kafka producer
			KafkaProducer<String, String> kafkaProducer = createKafkaProducer();

			Runtime.getRuntime().addShutdownHook(new Thread(() -> {
				logger.info("closing kafka producer");
				kafkaProducer.close();
			}));
			
			for (Tweet tw : tweets) {
				TweetData tweetData = new TweetData(tw.getId(), tw.getText(), tw.getLikeCount());
				kafkaProducer.send(new ProducerRecord<String, String>(TWITTER_TOPIC, null, OBJECT_MAPPER.writeValueAsString(tweetData)),
						new Callback() {

							@Override
							public void onCompletion(RecordMetadata metadata, Exception exception) {
								if (exception != null) {
									logger.error("Something went wrong", exception);
								}
							}
						});
			}
			
		  
		} catch (IOException e) {
			logger.error("Something went wrong", e);
		}
	}

	private KafkaProducer<String, String> createKafkaProducer() {
		String bootStrapServer = "localhost:9092";

		// Create properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// create producer
		KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

		return kafkaProducer;
	}

}
