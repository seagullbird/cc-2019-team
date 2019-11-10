package io.vertx.starter;

import org.json.JSONObject;

public class Q2User {

  private Long userId;

  private String description;

  private String screenName;

  private JSONObject hashtags;

  private double finalScore;

  private String latestContent;

  public Q2User(Long userId, String screenName, String description, JSONObject hashtags) {
    this.userId = userId;
    this.description = description;
    this.screenName = screenName;
    this.hashtags = hashtags;
    this.finalScore = 0.0;
  }

  public Long getUserId() {
    return userId;
  }

  public String getDescription() {
    return description;
  }

  public String getScreenName() {
    return screenName;
  }

  public JSONObject getHashtags() {
    return hashtags;
  }

  public void setFinalScore(double finalScore) {
    this.finalScore = finalScore;
  }

  public double getFinalScore() {
    return finalScore;
  }

  public String getLatestContent() {
    return latestContent;
  }

  public void setLatestContent(String latestContent) {
    this.latestContent = latestContent;
  }

  public String toString() {
    return String.join("-", userId.toString(), description, screenName, hashtags.toString());
  }
}
