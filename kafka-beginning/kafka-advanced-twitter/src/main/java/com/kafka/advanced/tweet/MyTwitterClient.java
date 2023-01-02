package com.kafka.advanced.tweet;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.redouane59.twitter.TwitterClient;
import com.github.redouane59.twitter.dto.tweet.Tweet;
import com.github.redouane59.twitter.dto.tweet.TweetSearchResponseV2;
import com.github.redouane59.twitter.helpers.AbstractRequestHelper;
import com.github.redouane59.twitter.helpers.ConverterHelper;
import com.github.redouane59.twitter.helpers.RequestHelper;
import com.github.redouane59.twitter.helpers.RequestHelperV2;
import com.github.redouane59.twitter.helpers.URLHelper;
import com.github.redouane59.twitter.signature.TwitterCredentials;
import com.github.scribejava.apis.TwitterApi;
import com.github.scribejava.core.builder.ServiceBuilder;
import com.github.scribejava.core.httpclient.HttpClient;
import com.github.scribejava.core.httpclient.HttpClientConfig;
import com.github.scribejava.core.oauth.OAuth10aService;

public class MyTwitterClient extends TwitterClient implements ITwiitterClient {

	public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
			.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
			.setSerializationInclusion(JsonInclude.Include.NON_NULL);
	private URLHelper urlHelper = new URLHelper();
	private RequestHelper requestHelperV1;
	private RequestHelperV2 requestHelperV2;
	private TwitterCredentials twitterCredentials;
	private static final String IDS = "ids";
	private static final String QUERY = "query";
	private static final String MAX_RESULTS = "max_results";
	private static final String USERS = "users";
	private static final String CURSOR = "cursor";
	private static final String NEXT = "next";
	private static final String PAGINATION_TOKEN = "pagination_token";
	private static final String NEXT_TOKEN = "next_token";
	private static final String RETWEET_COUNT = "retweet_count";
	private static final String RELATIONSHIP = "relationship";
	private static final String FOLLOWING = "following";
	private static final String FOLLOWED_BY = "followed_by";
	private static final String SOURCE = "source";
	private static final String NULL_OR_ID_NOT_FOUND_ERROR = "response null or ids not found !";
	private static final String[] DEFAULT_VALID_CREDENTIALS_FILE_NAMES = { "test-twitter-credentials.json",
			"twitter-credentials.json" };
	
	public MyTwitterClient() {
		this(getAuthentication());
	}

	public MyTwitterClient(TwitterCredentials credentials) {
		this(credentials, new ServiceBuilder(credentials.getApiKey()).apiSecret(credentials.getApiSecretKey()));
	}

	public MyTwitterClient(TwitterCredentials credentials, HttpClient httpClient) {
		this(credentials, new ServiceBuilder(credentials.getApiKey()).apiSecret(credentials.getApiSecretKey())
				.httpClient(httpClient));
	}

	public MyTwitterClient(TwitterCredentials credentials, HttpClient httpClient, HttpClientConfig config) {
		this(credentials, new ServiceBuilder(credentials.getApiKey()).apiSecret(credentials.getApiSecretKey())
				.httpClient(httpClient).httpClientConfig(config));
	}

	public MyTwitterClient(TwitterCredentials credentials, ServiceBuilder serviceBuilder) {
		this(credentials, serviceBuilder.apiKey(credentials.getApiKey()).apiSecret(credentials.getApiSecretKey())
				.build(TwitterApi.instance()));
	}

	public MyTwitterClient(TwitterCredentials credentials, OAuth10aService service) {
		super(credentials, service);
		twitterCredentials = credentials;
		requestHelperV1 = new RequestHelper(credentials, service);
		requestHelperV2 = new RequestHelperV2(credentials, service);
	}

	@Override
	public List<Tweet> searchForTweetsWithin7days(String query, Integer maxTweetCount, Integer batchCount) {
		int iterationCount = OptionalInt.of(batchCount).isPresent() ? batchCount : 1;
		int count = 100;
		Map<String, String> parameters = new HashMap<>();
		parameters.put(QUERY, query);
		parameters.put(MAX_RESULTS, String.valueOf(count));
		parameters.put("tweet.fields", URLHelper.ALL_TWEET_FIELDS);
		String next;
		List<Tweet> result = new ArrayList<>();
		do {
			Optional<TweetSearchResponseV2> tweetSearchV2DTO = this.getRequestHelper().getRequestWithParameters(
					URLHelper.SEARCH_TWEET_7_DAYS_URL, parameters, TweetSearchResponseV2.class);
			if (!tweetSearchV2DTO.isPresent() || tweetSearchV2DTO.get().getData() == null) {
				break;
			}

			result.addAll(tweetSearchV2DTO.get().getData());
			next = tweetSearchV2DTO.get().getMeta().getNextToken();
			parameters.put(NEXT_TOKEN, next);
			iterationCount--;
		} while (next != null && iterationCount > 0);
		return result;
	}

	private AbstractRequestHelper getRequestHelper() {
		if (this.requestHelperV1.getTwitterCredentials().getAccessToken() != null
				&& this.requestHelperV1.getTwitterCredentials().getAccessTokenSecret() != null) {
			return this.requestHelperV1;
		} else {
			return this.requestHelperV2;
		}
	}

}
