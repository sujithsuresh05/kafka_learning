package com.kafka.learning;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoWithThread {

	private static final String BOOTSTRAP_SERVER = "localhost:9092";
	private static Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class);
	private static String topic = "first-topic";
	private static String group = "second-group";

	public static void main(String args[]) {
		new ConsumerDemoWithThread().run();
	}

	private ConsumerDemoWithThread() {

	}

	private void run() {
		// create consumer configs
		Properties properties = new Properties();
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		// ConsumerConfig config = new ConsumerConfig(properties);

		// latch for dealing with multiple thread
		CountDownLatch latch = new CountDownLatch(1);
		ConsumerRunnable consumerRunnable = new ConsumerRunnable(properties, null);

		logger.info("creating the consumer thread!!");
		// start the thread
		Thread consumerThread = new Thread(consumerRunnable);
		consumerThread.start();

		// add a shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			logger.info("Caught shutdown hook");
			consumerRunnable.shutdown();
			try {
				latch.await();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			logger.info("Application has exited");
		}));

		try {
			latch.await();
		} catch (InterruptedException e) {
			logger.info("Application got Interrupted");
		} finally {
			logger.info("Application is closing");
		}
	}

	public class ConsumerRunnable implements Runnable {

		private KafkaConsumer<String, String> kafkaConsumer;
		private CountDownLatch latch;
		private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);

		public ConsumerRunnable(Properties properties, CountDownLatch latch) {
			// create consumer groups
			this.kafkaConsumer = new KafkaConsumer<>(properties);
			this.latch = latch;
		}

		@Override
		public void run() {

			// subscribe to a topic
			kafkaConsumer.subscribe(Collections.singleton(topic));

			try {
				// consume message
				while (true) {
					ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
					for (ConsumerRecord<String, String> rc : records) {
						logger.info("Key :" + rc.key() + " Value : " + rc.value());
						logger.info("Partition: " + rc.partition() + " offset: " + rc.offset());
					}
				}
			} catch (WakeupException e) {
				logger.error("Received shutdown signal!!");
			} finally {
				kafkaConsumer.close();
				// tell to our main code we are done with consumer
				latch.countDown();
			}

		}

		public void shutdown() {
			// wakeup is a special method to interrupt consumer.poll()
			// this will throw WakeUpException
			kafkaConsumer.wakeup();
		}

	}

}
