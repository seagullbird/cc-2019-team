package io.vertx.starter;


import io.vertx.core.json.JsonObject;

public class Q2Tweet {

  private Long senderUid;

  private Long tweetId;

  private Long targetUid;

  private JsonObject hashTags;

  private Long createdAt;

  private String text;

  public Q2Tweet(
    Long senderUid,
    Long tweetId,
    Long targetUid,
    JsonObject hashTags,
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

  public JsonObject getHashTags() {
    return hashTags;
  }

  public Long getCreatedAt() {
    return createdAt;
  }

  public String getText() {
    return text;
  }

}
