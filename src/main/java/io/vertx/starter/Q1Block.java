package io.vertx.starter;

import org.apache.commons.codec.digest.DigestUtils;

import java.util.List;
import java.util.StringJoiner;


class Q1Block {
  int id;
  String hash;
  List<Q1Transaction> all_tx;
  String pow;

  String calcHash(String preHash) {
    StringJoiner sj = new StringJoiner("|");
    sj.add(String.valueOf(id));
    sj.add(preHash);
    for (Q1Transaction tx : all_tx) {
      sj.add(tx.hash);
    }
    return OGCoin.ccHash(DigestUtils.sha256Hex(sj.toString()) + pow);
  }
}
