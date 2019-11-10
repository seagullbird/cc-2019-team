package io.vertx.starter;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.apache.commons.lang.StringUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import static io.vertx.starter.HTTPServerVerticle.respondMsg;

public class Query3 {
  private static final List<String> dirtyWords =
    Arrays.asList(
      "15619cctest",
      "4r5e",
      "5h1t",
      "5hit",
      "n1gga",
      "n1gger",
      "nobhead",
      "nobjocky",
      "nobjokey",
      "nutsack",
      "numbnuts",
      "nazi",
      "nigg3r",
      "nigg4h",
      "nigga",
      "niggas",
      "niggaz",
      "niggah",
      "nigger",
      "niggers",
      "omg",
      "p0rn",
      "poop",
      "pron",
      "prick",
      "pricks",
      "pussy",
      "pussys",
      "pusse",
      "pussi",
      "pussies",
      "pube",
      "pawn",
      "penis",
      "penisfucker",
      "phonesex",
      "phuq",
      "phuck",
      "phuk",
      "phuks",
      "phuked",
      "phuking",
      "phukked",
      "phukking",
      "piss",
      "pissoff",
      "pisser",
      "pissers",
      "pisses",
      "pissflaps",
      "pissin",
      "pissing",
      "pigfucker",
      "pimpis",
      "queer",
      "rectum",
      "rimjaw",
      "rimming",
      "snatch",
      "sonofabitch",
      "spunk",
      "scrotum",
      "scrote",
      "scroat",
      "schlong",
      "sex",
      "semen",
      "sh1t",
      "shit",
      "shits",
      "shitty",
      "shitter",
      "shitters",
      "shitted",
      "shitting",
      "shittings",
      "shitdick",
      "shite",
      "shitey",
      "shited",
      "shitfuck",
      "shitfull",
      "shithead",
      "shiting",
      "shitings",
      "skank",
      "slut",
      "smut",
      "smegma",
      "tosser",
      "turd",
      "tw4t",
      "twunt",
      "twunter",
      "twat",
      "twatty",
      "twathead",
      "teets",
      "tit",
      "tittywank",
      "tittyfuck",
      "titties",
      "tittiefucker",
      "titwank",
      "titfuck",
      "v14gra",
      "v1gra",
      "vulva",
      "vagina",
      "viagra",
      "w00se",
      "wtff",
      "wang",
      "wank",
      "wanky",
      "wanker",
      "whore",
      "whore4r5e",
      "whoreshit",
      "whoreanal",
      "whoar",
      "a55",
      "anus",
      "anal",
      "arse",
      "ass",
      "assram",
      "asswhole",
      "assfucker",
      "assfukka",
      "assho",
      "b00bs",
      "b17ch",
      "b1tch",
      "boner",
      "booooooobs",
      "booooobs",
      "boooobs",
      "booobs",
      "boob",
      "boobs",
      "boiolas",
      "bollock",
      "bollok",
      "breasts",
      "bunnyfucker",
      "butt",
      "buttplug",
      "buttmuch",
      "buceta",
      "bugger",
      "bullshit",
      "bum",
      "bastard",
      "balls",
      "ballsack",
      "bestial",
      "bestiality",
      "beastial",
      "beastiality",
      "bellend",
      "bitch",
      "biatch",
      "bloody",
      "blowjob",
      "blowjobs",
      "c0ck",
      "c0cksucker",
      "cnut",
      "coon",
      "cox",
      "cock",
      "cocks",
      "cocksuck",
      "cocksucks",
      "cocksucker",
      "cocksucked",
      "cocksucking",
      "cocksuka",
      "cocksukka",
      "cockface",
      "cockhead",
      "cockmunch",
      "cockmuncher",
      "cok",
      "coksucka",
      "cokmuncher",
      "crap",
      "cunnilingus",
      "cunt",
      "cunts",
      "cuntlick",
      "cuntlicker",
      "cuntlicking",
      "cunilingus",
      "cunillingus",
      "cum",
      "cums",
      "cumshot",
      "cummer",
      "cumming",
      "cyalis",
      "cyberfuc",
      "cyberfuck",
      "cyberfucker",
      "cyberfuckers",
      "cyberfucked",
      "cyberfucking",
      "carpetmuncher",
      "cawk",
      "chink",
      "cipa",
      "cl1t",
      "clit",
      "clitoris",
      "clits",
      "d1ck",
      "donkeyribber",
      "doosh",
      "dogfucker",
      "doggin",
      "dogging",
      "duche",
      "dyke",
      "damn",
      "dink",
      "dinks",
      "dirsa",
      "dick",
      "dickhead",
      "dildo",
      "dildos",
      "dlck",
      "ejaculate",
      "ejaculates",
      "ejaculated",
      "ejaculating",
      "ejaculatings",
      "ejaculation",
      "ejakulate",
      "f4nny",
      "fook",
      "fooker",
      "fux",
      "fux0r",
      "fuck",
      "fucks",
      "fuckwhit",
      "fuckwit",
      "fucka",
      "fucker",
      "fuckers",
      "fucked",
      "fuckhead",
      "fuckheads",
      "fuckin",
      "fucking",
      "fuckings",
      "fuckingshitmotherfucker",
      "fuckkkdatttbitchhhh",
      "fuckme",
      "fudgepacker",
      "fuk",
      "fuks",
      "fukwhit",
      "fukwit",
      "fuker",
      "fukker",
      "fukkin",
      "fannyfucker",
      "fannyflaps",
      "fanyy",
      "fag",
      "fagot",
      "fagots",
      "fags",
      "faggot",
      "faggs",
      "fagging",
      "faggitt",
      "fcuk",
      "fcuker",
      "fcuking",
      "feck",
      "fecker",
      "felching",
      "fellate",
      "fellatio",
      "fingerfuck",
      "fingerfucks",
      "fingerfucker",
      "fingerfuckers",
      "fingerfucked",
      "fingerfucking",
      "fistfuck",
      "fistfucks",
      "fistfucker",
      "fistfuckers",
      "fistfucked",
      "fistfucking",
      "fistfuckings",
      "flange",
      "goatse",
      "goddamn",
      "gangbang",
      "gangbangs",
      "gangbanged",
      "gaysex",
      "horny",
      "horniest",
      "hore",
      "hotsex",
      "hoar",
      "hoare",
      "hoer",
      "homo",
      "hardcoresex",
      "heshe",
      "hell",
      "jap",
      "jackoff",
      "jerk",
      "jerkoff",
      "jism",
      "jiz",
      "jizz",
      "jizm",
      "knob",
      "knobend",
      "knobead",
      "knobed",
      "knobhead",
      "knobjocky",
      "knobjokey",
      "kondum",
      "kondums",
      "kock",
      "kunilingus",
      "kum",
      "kums",
      "kummer",
      "kumming",
      "kawk",
      "l3itch",
      "l3ich",
      "lust",
      "lusting",
      "labia",
      "lmao",
      "lmfao",
      "m0f0",
      "m0fo",
      "m45terbate",
      "mothafuck",
      "mothafucks",
      "mothafucka",
      "mothafuckas",
      "mothafuckaz",
      "mothafucker",
      "mothafuckers",
      "mothafucked",
      "mothafuckin",
      "mothafucking",
      "mothafuckings",
      "motherfuck",
      "motherfucks",
      "motherfucker",
      "motherfuckers",
      "motherfucked",
      "motherfuckin",
      "motherfucking",
      "motherfuckings",
      "motherfuckka",
      "mof0",
      "mofo",
      "mutha",
      "muthafuckker",
      "muthafecker",
      "muther",
      "mutherfucker",
      "muff",
      "ma5terb8",
      "ma5terbate",
      "masochist",
      "masturbate",
      "masterb8",
      "masterbat",
      "masterbat3",
      "masterbate",
      "masterbation",
      "masterbations");


  static void run(Vertx vertx, RoutingContext context) {
    HttpServerRequest request = context.request();
    String uidStartStr = request.getParam("uid_start");
    String uidEndStr = request.getParam("uid_end");
    String timeStartStr = request.getParam("time_start");
    String timeEndStr = request.getParam("time_end");
    String n1Str = request.getParam("n1");
    String n2Str = request.getParam("n2");

    String header = HTTPServerVerticle.header;
    long uidStart;
    long uidEnd;
    long timeStart;
    long timeEnd;
    int n1;
    int n2;

    // check validity
    try {
      uidStart = Long.parseLong(uidStartStr);
      uidEnd = Long.parseLong(uidEndStr);
      timeStart = Long.parseLong(timeStartStr);
      timeEnd = Long.parseLong(timeEndStr);
      n1 = Integer.parseInt(n1Str);
      n2 = Integer.parseInt(n2Str);
    } catch (Exception e) {
      e.printStackTrace();
      respondMsg(context, header);
      return;
    }

    if (uidStart > uidEnd || timeStart > timeEnd || n1 == 0) {
      respondMsg(context, header);
      return;
    }

    Handler<AsyncResult<Message<Object>>> handler =
      reply -> handleFirstQuery(vertx, context, reply, header, n1, n2);
    vertx
      .eventBus()
      .request(
        MainVerticle.dbQueue,
        new JsonObject()
          .put("uidStart", uidStartStr)
          .put("uidEnd", uidEndStr)
          .put("timeStart", timeStartStr)
          .put("timeEnd", timeEndStr),
        new DeliveryOptions().addHeader("action", "words"),
        handler);
  }

   private static void handleFirstQuery(Vertx vertx,
      RoutingContext context, AsyncResult<Message<Object>> reply, String header, int n1, int n2) {
     if (!reply.succeeded()) {
       context.fail(reply.cause());
       return;
     }
     // word -> set of tweet ids which contains this word
     Map<String, Set<Long>> words2tweetIds = new HashMap<>();
     // word -> topic scores
     Map<String, Double> topicScores = new HashMap<>();
     Map<String, Double> word2sum = new HashMap<>();

     JsonArray body = (JsonArray) reply.result().body();
     body.forEach(
       rawRow -> {
         JsonArray row = (JsonArray) rawRow;
         long id = row.getLong(0);
         // word -> occurrence / twc * Math.log(impactScore + 1)
         JsonObject words = new JsonObject(row.getString(1));
         for (String sumStr : words.fieldNames()) {
           double sum = Double.parseDouble(sumStr);
           for (Object wordObj : words.getJsonArray(sumStr)) {
             String word = (String) wordObj;
             word2sum.put(word, word2sum.getOrDefault(word, 0D) + sum);
             words2tweetIds.putIfAbsent(word, new HashSet<>());
             words2tweetIds.get(word).add(id);
           }
         }
       });
     int numInRange = body.size();
     for (String word : words2tweetIds.keySet()) {
       topicScores.put(
         word,
         word2sum.get(word) * Math.log(numInRange / (double) words2tweetIds.get(word).size()));
     }

     StringJoiner resultSj = new StringJoiner("\n");
     resultSj.add(header);
     // sort topic scores
     List<Map.Entry<String, Double>> topicScoreResult =
       topicScores.entrySet().stream()
         .sorted(
           (entry2, entry1) -> {
             if (!entry1.getValue().equals(entry2.getValue())) {
               return entry1.getValue().compareTo(entry2.getValue());
             }
             return entry2.getKey().compareTo(entry1.getKey());
           })
         .limit(n1)
         .collect(Collectors.toList());
     StringJoiner topicScoreSj = new StringJoiner("\t");
     for (Map.Entry<String, Double> result : topicScoreResult) {
       topicScoreSj.add(
         String.join(":", censorWord(result.getKey()), String.format("%.2f", result.getValue())));
     }
     resultSj.add(topicScoreSj.toString());

     if (n2 == 0) {
       respondMsg(context, resultSj.toString());
       return;
     }
     // get related tweets
     Set<Long> relatedTweetIds = new HashSet<>();
     topicScoreResult.forEach(entry -> relatedTweetIds.addAll(words2tweetIds.get(entry.getKey())));
     if (relatedTweetIds.size() == 0) {
       respondMsg(context, resultSj.toString());
       return;
     }

     StringJoiner sj = new StringJoiner(",", "(", ")");
     relatedTweetIds.forEach(id -> sj.add(id.toString()));

     Handler<AsyncResult<Message<Object>>> handler =
       reply2 -> handleSecondQuery(context, reply2, resultSj);
     vertx
       .eventBus()
       .request(
         MainVerticle.dbQueue,
         new JsonObject().put("ids", sj.toString()).put("n2", String.valueOf(n2)),
         new DeliveryOptions().addHeader("action", "texts"),
         handler);
  }

  private static void handleSecondQuery(
    RoutingContext context, AsyncResult<Message<Object>> reply, StringJoiner resultSj) {
    if (!reply.succeeded()) {
      context.fail(reply.cause());
      return;
    }

    JsonArray body = (JsonArray) reply.result().body();
    body.forEach(
        rawRow -> {
          JsonArray row = (JsonArray) rawRow;
          JsonArray censoredTextWrap = new JsonArray(row.getString(2));
          System.out.println(censoredTextWrap);
          resultSj.add(
              String.join(
                  "\t",
                  String.valueOf(row.getLong(0)),
                  String.valueOf(row.getLong(1)),
                  censoredTextWrap.getString(0)));
        });
    respondMsg(context, resultSj.toString());
  }

  private static String censorWord(String word) {
    if (!dirtyWords.contains(word)) return word;
    String firstChar = String.valueOf(word.charAt(0));
    String lastChar = String.valueOf(word.charAt(word.length() - 1));
    String middle = StringUtils.repeat("*", word.length() - 2);
    return firstChar + middle + lastChar;
  }
}
