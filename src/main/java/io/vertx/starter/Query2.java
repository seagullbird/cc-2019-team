package io.vertx.starter;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.RoutingContext;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.vertx.starter.HTTPServerVerticle.respondMsg;

public class Query2 {
  private static final Logger LOGGER = LoggerFactory.getLogger(DBMySqlVerticle.class);
  private static final Set<String> VALID_TYPES =
      new HashSet<String>() {
        {
          add("retweet");
          add("reply");
          add("both");
        }
      };

  static void run(Vertx vertx, RoutingContext context) {
    HttpServerRequest request = context.request();
    String userId = request.getParam("user_id");
    String type = request.getParam("type");
    String phrase = request.getParam("phrase");
    String hashtag = request.getParam("hashtag");

    if (!VALID_TYPES.contains(type)) {
      respondMsg(context, HTTPServerVerticle.header);
      return;
    }

    try {
      final String phraseDecoded = URLDecoder.decode(phrase, "UTF-8");
      final String hashtagLower = hashtag.toLowerCase(Locale.ENGLISH);
      final Long reqUser = Long.parseLong(userId);
      Handler<AsyncResult<Message<Object>>> handler =
          reply -> handlerQuery(context, reply, reqUser, type, phraseDecoded, hashtagLower);
      vertx
          .eventBus()
          .request(
              MainVerticle.dbQueue,
              new JsonObject().put("user", reqUser),
              new DeliveryOptions().addHeader("action", "q2"),
              handler);
    } catch (UnsupportedEncodingException e) {
      respondMsg(context, HTTPServerVerticle.header);
      LOGGER.error(e.toString());
    }
  }

  private static void handlerQuery(
      RoutingContext context,
      AsyncResult<Message<Object>> reply,
      Long reqUser,
      String type,
      String phrase,
      String hashtag) {
    if (!reply.succeeded()) {
      context.fail(reply.cause());
      return;
    }
    // user -> user info
    Map<Long, Q2User> userMap = new HashMap<>();
    // user -> retweet counts with request user (for interaction)
    Map<Long, Integer> retweetCounts = new HashMap<>();
    // user -> reply counts with request user (for interaction)
    Map<Long, Integer> replyCounts = new HashMap<>();
    // user -> latest tweet
    Map<Long, Q2Tweet> latestTweets = new HashMap<>();
    // user -> numOfMatch (keyword)
    Map<Long, Integer> keywordNumOfMatches = new HashMap<>();

    JsonArray body = (JsonArray) reply.result().body();
    if (body.size() != 0) {
      JsonArray row = body.getJsonArray(0);
      String description = getFromJsonArray(row.getString(0));
      String screen_name = getFromJsonArray(row.getString(1));
      JsonObject user_tags = getUtags(row);
      userMap.put(reqUser, new Q2User(reqUser, screen_name, description, user_tags));

      JsonArray records = new JsonArray(row.getString(3));
      for (int idx = 0; idx < records.size(); idx++) {
        JsonArray record = records.getJsonArray(idx);
        Long u2_id = record.getLong(0);
        String u2_desc = record.getString(1);
        String u2_name = record.getString(2);
        JsonObject u2_tags = record.getJsonObject(3);

        // gen userMap
        userMap.put(u2_id, new Q2User(u2_id, u2_name, u2_desc, u2_tags));

        JsonArray tweets = record.getJsonArray(4);
        for (int j = 0; j < tweets.size(); j++) {
          JsonArray tweet = tweets.getJsonArray(j);
          int tweetType = tweet.getInteger(1);

          // check type
          if (type.equals("reply") && tweetType == 0) {
            continue;
          }
          if (type.equals("retweet") && tweetType == 1) {
            continue;
          }
          Long tweetId = tweet.getLong(0);
          Long createdAt = tweet.getLong(2);
          JsonObject hashTags = tweet.getJsonObject(3);
          String text = tweet.getString(4);

          // prepare to calculate interaction score
          if (tweetType == 0) {
            retweetCounts.put(u2_id, retweetCounts.getOrDefault(u2_id, 0) + 1);
          } else if (tweetType == 1) {
            replyCounts.put(u2_id, replyCounts.getOrDefault(u2_id, 0) + 1);
          }

          // new Tweet(uid1, tweetId, uid2, hashTags, createdAt, content)
          int numOfTags = hashTags.getInteger(hashtag, 0);
          int numOfPhrase = 0;
          int init = text.indexOf(phrase);
          while (init != -1) {
            numOfPhrase++;
            init = text.indexOf(phrase, init + 1);
          }
          keywordNumOfMatches.put(
              u2_id, keywordNumOfMatches.getOrDefault(u2_id, 0) + numOfTags + numOfPhrase);
          // to update latest tweet
          Q2Tweet thisTweet = new Q2Tweet(reqUser, tweetId, u2_id, hashTags, createdAt, text);
          Q2Tweet currentLatestTweet = latestTweets.get(u2_id);
          if (currentLatestTweet == null) {
            latestTweets.put(u2_id, thisTweet);
          } else {
            long currentLatestTimestamp = currentLatestTweet.getCreatedAt();
            long thisTimestamp = thisTweet.getCreatedAt();
            if ((thisTimestamp > currentLatestTimestamp)
                || (thisTimestamp == currentLatestTimestamp
                    && thisTweet.getTweetId() > currentLatestTweet.getTweetId())) {
              latestTweets.put(u2_id, thisTweet);
            }
          }
        }
      }
    }

    // calculate final scores
    List<Q2User> result = new ArrayList<>();
    Q2User reqUserObj = userMap.get(reqUser);
    if (reqUserObj != null) {
      JsonObject requestUserHashtags = reqUserObj.getHashtags();
      if (requestUserHashtags != null) {
        for (Long uid : userMap.keySet()) {
          double interactionScore =
              Math.log(
                  1 + 2 * replyCounts.getOrDefault(uid, 0) + retweetCounts.getOrDefault(uid, 0));
          if (interactionScore == 0) {
            continue;
          }
          // calculate keyword score
          double keywordScore = Math.log(keywordNumOfMatches.getOrDefault(uid, 0) + 1.0) + 1.0;
          // calculate hash score
          double hashtagScore = 1.0;
          if (!uid.equals(reqUser)) {
            int count = 0;
            JsonObject contactUserHashtags = userMap.get(uid).getHashtags();
            for (String tag : requestUserHashtags.fieldNames()) {
              if (contactUserHashtags.getInteger(tag, 0) > 0) {
                count += contactUserHashtags.getInteger(tag) + requestUserHashtags.getInteger(tag);
              }
            }
            if (count > 10) {
              hashtagScore = Math.log(1 + count - 10) + 1.0;
            }
          }
          double finalScore =
              Double.parseDouble(
                  String.format("%.5f", interactionScore * keywordScore * hashtagScore));

          if (finalScore != 0) {
            Q2User resultUser = userMap.get(uid);
            resultUser.setFinalScore(finalScore);
            resultUser.setLatestContent(latestTweets.get(uid).getText());
            result.add(resultUser);
          }
        }
      }
    }

    List<String> ans =
        result.stream()
            .sorted(
                (user1, user2) -> {
                  if (user1.getFinalScore() == user2.getFinalScore()) {
                    return user2.getUserId().compareTo(user1.getUserId());
                  } else {
                    return Double.compare(user2.getFinalScore(), user1.getFinalScore());
                  }
                })
            .map(
                user -> {
                  List<String> items = new ArrayList<>();
                  items.add(user.getUserId().toString());
                  items.add(user.getScreenName());
                  items.add(user.getDescription());
                  items.add(user.getLatestContent());
                  return String.join("\t", items);
                })
            .collect(Collectors.toList());

    if (ans.isEmpty()) {
      respondMsg(context, HTTPServerVerticle.header);
    } else {
      respondMsg(context, HTTPServerVerticle.header + "\n" + String.join("\n", ans));
    }
  }

  private static String getFromJsonArray(String jsonStr) {
    try {
      JsonArray json = new JsonArray(jsonStr);
      return json.getString(0);
    } catch (Exception e) {
      return "";
    }
  }

  private static JsonObject getUtags(JsonArray row) {
    try {
      return new JsonObject(row.getString(2));
    } catch (Exception e) {
      return null;
    }
  }
}
