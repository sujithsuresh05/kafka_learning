package com.kafka.advanced.tweet;

import java.util.List;

import com.github.redouane59.twitter.ITwitterClientV2;
import com.github.redouane59.twitter.dto.tweet.Tweet;

public interface ITwiitterClient extends ITwitterClientV2{

	public List<Tweet> searchForTweetsWithin7days(String query, Integer maxTweetCount, Integer batchCount);
}
