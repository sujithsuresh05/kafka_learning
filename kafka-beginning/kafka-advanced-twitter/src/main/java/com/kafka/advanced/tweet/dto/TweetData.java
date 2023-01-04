package com.kafka.advanced.tweet.dto;

public class TweetData {

	private String id;
	private String data;
	private int likeCount;

	public TweetData(String id, String data, int likeCount) {
		super();
		this.id = id;
		this.data = data;
		this.likeCount = likeCount;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getData() {
		return data;
	}

	public void setData(String data) {
		this.data = data;
	}

	public int getLikeCount() {
		return likeCount;
	}

	public void setLikeCount(int likeCount) {
		this.likeCount = likeCount;
	}

}
