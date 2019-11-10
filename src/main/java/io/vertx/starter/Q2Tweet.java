package io.vertx.starter;

import org.json.JSONObject;

public class Q2Tweet {

  private Long senderUid;

  private Long tweetId;

  private Long targetUid;

  private JSONObject hashTags;

  private Long createdAt;

  private String text;

  public Q2Tweet(
      Long senderUid,
      Long tweetId,
      Long targetUid,
      JSONObject hashTags,
      Long createdAt,
      String text) {
    this.senderUid = senderUid;
    this.tweetId = tweetId;
    this.targetUid = targetUid;
    this.hashTags = hashTags;
    this.createdAt = createdAt;
    this.text = text;
  }

  public Long getSenderUid() {
    return senderUid;
  }

  public Long getTweetId() {
    return tweetId;
  }

  public Long getTargetUid() {
    return targetUid;
  }

  public JSONObject getHashTags() {
    return hashTags;
  }

  public Long getCreatedAt() {
    return createdAt;
  }

  public String getText() {
    return text;
  }
}
