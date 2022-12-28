package com.kafka.learning;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Kafka Producer
 *
 */
public class ProducerDemo {
	private static final String BOOTSTRAP_SERVER = "localhost:9092";
	
	

	public static void main(String[] args) {
		// Create producer config
		Properties producerConfigProperties = new Properties();
		producerConfigProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
		producerConfigProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		producerConfigProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// Create Producer Record
		ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("first-topic", "hello my first message from java application" );
		
		// Create Producer
		KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(producerConfigProperties);
		
		// Send Data
		kafkaProducer.send(producerRecord);
		kafkaProducer.flush();
		kafkaProducer.close();
	}
}
