package com.kafka.learning;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.message.ConsumerProtocolAssignment.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoAssignSeek {

	private static final String BOOTSTRAP_SERVER = "localhost:9092";
	private static Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
	private static String topic = "first-topic";
	private static String group = "first-group-assign-seek";

	public static void main(String args[]) {

		// create consumer configs
		Properties properties = new Properties();
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		// ConsumerConfig config = new ConsumerConfig(properties);

		// create consumer groups
		KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

		// assign and seek mostly used to replay data or fetch a slecific message
		
		// assign
		TopicPartition topicPartition = new TopicPartition();
		topicPartition.setPartitions(null);
		
		// subscribe to a topic
		kafkaConsumer.subscribe(Collections.singleton(topic));

		// consume message
		while (true) {
			ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
		    for(ConsumerRecord<String, String> rc : records) {
		    	logger.info("Key :" + rc.key() + " Value : " + rc.value());
		    	logger.info("Partition: " + rc.partition() + " offset: " + rc.offset());
		    }
		}
	}

}
