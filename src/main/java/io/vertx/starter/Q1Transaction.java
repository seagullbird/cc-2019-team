package io.vertx.starter;

import java.math.BigInteger;
import java.util.StringJoiner;

class Q1Transaction {
  Long recv;
  Long amt;
  String time;
  String hash;
  Long send;
  Long fee;
  BigInteger sig;

  String calcHash() {
    StringJoiner sj = new StringJoiner("|");
    sj.add(String.valueOf(time));
    if (send != null) sj.add(String.valueOf(send));
    else sj.add("");
    sj.add(String.valueOf(recv));
    sj.add(String.valueOf(amt));
    if (fee != null) sj.add(String.valueOf(fee));
    else sj.add("");
    return OGCoin.ccHash(sj.toString());
  }

  boolean verifySig() {
    if (sig == null) return true;
    long n = send;
    long txHashInt = Long.parseLong(hash, 16);
    return BigInteger.valueOf(txHashInt)
      .equals(sig.modPow(BigInteger.valueOf(OGCoin.key_e), BigInteger.valueOf(n)));
  }

  void sign() {
    long txHashInt = Long.parseLong(hash, 16);
    sig =
      BigInteger.valueOf(txHashInt)
        .modPow(BigInteger.valueOf(OGCoin.key_d), BigInteger.valueOf(OGCoin.key_n));
  }
}
