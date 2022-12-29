package com.kafka.basics;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka Producer
 *
 */
public class ProducerDemoWithCallback {
	private static final String BOOTSTRAP_SERVER = "localhost:9092";
	private static Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

	public static void main(String[] args) {
		// Create producer config
		Properties producerConfigProperties = new Properties();
		producerConfigProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
		producerConfigProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		producerConfigProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// Create Producer
		KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(producerConfigProperties);
		for (int i = 0; i < 10; i++) {
			// Create Producer Record
			ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("first-topic",
					"My message " + i + " from java application");

			// Send Data
			kafkaProducer.send(producerRecord, new Callback() {

				@Override
				public void onCompletion(RecordMetadata metadata, Exception e) {
					if (e == null) {
						logger.info("Recieved new metadat \n" + "Topic: " + metadata.topic() + "\n" + "Partition: "
								+ metadata.partition() + "\n" + "Offset: " + metadata.offset() + "\n" + "TimeStamp: "
								+ metadata.timestamp());
					} else {
						logger.error("Error while sending ", e);
					}

				}
			});
		}

		kafkaProducer.flush();
		kafkaProducer.close();
	}
}
