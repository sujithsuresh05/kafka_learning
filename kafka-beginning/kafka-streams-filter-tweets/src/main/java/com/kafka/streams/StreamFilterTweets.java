package com.kafka.streams;

import java.util.Properties;

import org.apache.kafka.streams.StreamsConfig;

public class StreamFilterTweets {
	
	private static final String bootstrapServer = "localhost:9092";

	public static void main(String args) {
		
		// create properties
		Properties properties = new Properties();
		properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
	}
}
