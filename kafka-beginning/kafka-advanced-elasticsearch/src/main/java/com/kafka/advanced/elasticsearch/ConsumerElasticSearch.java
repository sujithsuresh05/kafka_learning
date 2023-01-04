package com.kafka.advanced.elasticsearch;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.advanced.elastic.dto.TweetData;

/*
 * 
 */
public class ConsumerElasticSearch {

	private static Logger logger = LoggerFactory.getLogger(ConsumerElasticSearch.class.getName());
	private static final String BOOTSTRAP_SERVER = "localhost:9092";
	private static String topic = "twitter_topic_kafka";
	private static String group = "twitter_group";
	public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
			.setSerializationInclusion(JsonInclude.Include.NON_NULL)
			.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

	public static void main(String[] args) throws IOException {
		final RestHighLevelClient restHighLevelClient = createClient();

		// create consumer groups
		final KafkaConsumer<String, String> kafkaConsumer = createKafkaConsumer();

		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			// close the client grace fully
			try {
				restHighLevelClient.close();
				kafkaConsumer.close();
			} catch (IOException e) {
				logger.error("Error occurred", e);
			}
		}));

		// consume message
		// track id ZbvBd4UBIMpiuYQ3kPWE
		while (true) {
			ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));

			// further improvement adding bulk request

			BulkRequest bulkRequest = new BulkRequest();
			Integer recordCount = records.count();
			logger.info("Records count : " + recordCount);

			for (ConsumerRecord<String, String> rc : records) {
				if (rc.value().isEmpty())
					continue;
				// logger.info("Key :" + rc.key() + " Value : " + rc.value());
				// logger.info("Partition: " + rc.partition() + " offset: " + rc.offset());
				// 2 startegies to create idempotent consumer
				// String id = topic + "_" + rc.partition() +"_" + rc.offset()
				// repeating id 1610251668330799106
				String twid = getTweetId(rc.value());
				IndexRequest indexRequest = new IndexRequest("twitter", "tweets", twid // this is to make our consumer
																						// idempotent
				).source(rc.value(), XContentType.JSON);

//				IndexResponse indexResponse = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
//				String id = indexResponse.getId();
//				logger.info("created id :" + id);

				bulkRequest.add(indexRequest);
			}
			if (recordCount > 0) {
				BulkResponse bulkResponse = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
				logger.info("commiting offset...");
				kafkaConsumer.commitSync();
				logger.info("committed offset sync");
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

		}

	}

	public static String getTweetId(String tweet) throws JsonMappingException, JsonProcessingException {
		TweetData tweetData = OBJECT_MAPPER.readValue(tweet, TweetData.class);
		return tweetData.getId();
	}

	public static RestHighLevelClient createClient() {

		// https://app.bonsai.io/
		// replace with your own access url get from bonsai
		// https://9c5qscdv9s:6dqu4rqxvq@testing-kafka-8078140501.us-east-1.bonsaisearch.net:443
		// sample created id for me KmbOdoUBiMJ7ud-oztJa
		String userName = "9c5qscdv9s";
		String password = "6dqu4rqxvq";
		String hostName = "testing-kafka-8078140501.us-east-1.bonsaisearch.net";

		// don't do if you run local ES
		final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
		credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName, password));

		RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost(hostName, 443, "https"))
				.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {

					@Override
					public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
						// TODO Auto-generated method stub
						return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
					}
				});
		RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);

		return restHighLevelClient;
	}

	public static KafkaConsumer<String, String> createKafkaConsumer() {
		// create consumer configs
		Properties properties = new Properties();
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // this will disable auto commit of
																					// offsets
		properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "20");

		// ConsumerConfig config = new ConsumerConfig(properties);

		// create consumer groups
		KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
		// subscribe to a topic
		kafkaConsumer.subscribe(Collections.singleton(topic));

		return kafkaConsumer;
	}
}
