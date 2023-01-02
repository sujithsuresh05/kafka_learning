package com.kafka.advanced.twitter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.github.redouane59.twitter.TwitterClient;
import com.github.redouane59.twitter.dto.tweet.Tweet;
import com.github.redouane59.twitter.signature.TwitterCredentials;
import com.kafka.advanced.tweet.MyTwitterClient;

public class TweetFetcher {
	
	private MyTwitterClient myTwitterClient;

	public TweetFetcher(String credetialPath) throws JsonParseException, JsonMappingException, IOException {
		myTwitterClient = new MyTwitterClient(TwitterClient.OBJECT_MAPPER.readValue(new File(credetialPath), TwitterCredentials.class));

	}

	public List<Tweet> searchForTweets(String query) {
		List<Tweet> tweets = new ArrayList<>();
//		List<Tweet> tweets = myTwitterClient.searchForTweetsWithin7days(query, 100, 2);
//		Optional.ofNullable(tweets).ifPresent(twt -> twt.stream().forEach(tweet -> {
//			System.out.println(tweet.getText());
//		}));
		Optional.ofNullable(myTwitterClient.searchForTweetsWithin7days(query, 100, 2)).ifPresent(twt -> tweets.addAll(twt));
		return tweets;
	}

}
