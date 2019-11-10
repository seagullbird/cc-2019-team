package io.vertx.starter;

import com.jsoniter.JsonIterator;
import com.jsoniter.any.Any;
import com.jsoniter.spi.TypeLiteral;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.RandomStringUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.StringJoiner;
import java.util.zip.InflaterInputStream;

public class OGCoin {
  static final long key_n = 1284110893049L;
  static final long key_e = 15619L;
  static final long key_d = 379666662787L;
  private final long TEAM_ACCOUNT = key_n;

  private final String raw_input;
  private List<Q1Block> chain = new ArrayList<>();
  private Q1Transaction newTx;

  public OGCoin(String raw_input) {
    this.raw_input = raw_input;
  }

  /**
   * Verify the given input, mine a block, and produce a response.
   *
   * @return Response.
   */
  public String run() {
    try {
      parseInput();
      if (!verifyChain()) {
        return response("<INVALID|" + "chain verification failed>");
      }
      // success
      return response(mine());
    } catch (Exception e) {
      return response("<INVALID|" + e.getMessage() + ">");
    }
  }

  private String response(String message) {
    return HTTPServerVerticle.header + "\n" + message;
  }

  /**
   * Add the new tx to blockchain.
   *
   * @return A string in the format "<sig|pow>"
   */
  private String mine() {
    StringJoiner sj = new StringJoiner("|", "<", ">");
    updateNewTx();
    sj.add(String.valueOf(newTx.sig));

    // create reward transaction
    Q1Transaction rewardTx = new Q1Transaction();
    rewardTx.recv = key_n;
    rewardTx.time =
      String.valueOf(BigInteger.valueOf(600000000000L).add(new BigInteger(newTx.time)));
    rewardTx.amt = 500000000L;
    rewardTx.hash = rewardTx.calcHash();

    // create the new block
    Q1Block lastBlock = chain.get(chain.size() - 1);
    Q1Block newBlock = new Q1Block();
    newBlock.id = lastBlock.id + 1;
    newBlock.all_tx = Arrays.asList(newTx, rewardTx);

    // pow, calculate new hash
    sj.add(doPow(newBlock, lastBlock.hash));
    return sj.toString();
  }

  private String doPow(Q1Block block, String preHash) {
    String pow;
    String hash;
    do {
      pow = RandomStringUtils.randomAlphanumeric(new Random().nextInt(50));
      block.pow = pow;
      hash = block.calcHash(preHash);
    } while (hash.charAt(0) != '0');
    return pow;
  }

  /**
   * Given the new transaction received, update it with missing fields, including send, fee, hash
   * and sig.
   */
  private void updateNewTx() {
    // fill in send, fee, sig and hash
    newTx.fee = 0L;
    newTx.send = key_n;
    newTx.hash = newTx.calcHash();
    newTx.sign();
  }

  /**
   * Verify the given blockchain.
   *
   * @return true if valid, false otherwise.
   */
  private boolean verifyChain() {
    String preHash = "00000000";
    String lastTimestamp = "0";
    Map<Long, Long> balanceOf = new HashMap<>();

    int curBlockId = 0;
    for (Q1Block block : chain) {
      if (block.id != curBlockId) return false;
      curBlockId += 1;

      long totalFee = 0;
      List<Q1Transaction> allTx = block.all_tx;
      for (Q1Transaction tx : allTx) {
        // verify timestamp asc
        if (tx.time.compareTo(lastTimestamp) <= 0) return false;
        lastTimestamp = tx.time;

        // verify fee
        if (tx.fee != null) {
          long txFee = tx.fee;
          if (txFee < 0) return false;
          totalFee += txFee;
        }

        // verify amt
        long txAmt = tx.amt;
        if (txAmt < 0) return false;

        if (tx.send != null) {
          long sendAcc = tx.send;
          if (!balanceOf.containsKey(sendAcc)) {
            balanceOf.put(sendAcc, 0L);
          }

          // update sender balance
          balanceOf.put(sendAcc, balanceOf.get(sendAcc) - txAmt - tx.fee);
          // check sender balance
          if (balanceOf.get(sendAcc) < 0) return false;
        }
        // update receiver balance
        long recvAcc = tx.recv;
        if (!balanceOf.containsKey(recvAcc)) {
          balanceOf.put(recvAcc, 0L);
        }
        balanceOf.put(recvAcc, balanceOf.get(recvAcc) + txAmt);

        if (!tx.calcHash().equals(tx.hash)) return false;
        if (!tx.verifySig()) return false;
      }

      // verify reward
      Q1Transaction rewardTx = allTx.get(allTx.size() - 1);
      if (rewardTx.amt > 500000000L) return false;
      // miner earns total fee
      long miner = rewardTx.recv;
      balanceOf.put(miner, balanceOf.get(miner) + totalFee);

      // verify block
      if (block.hash.charAt(0) != '0' || !block.calcHash(preHash).equals(block.hash)) return false;
      preHash = block.hash;
    }

    // verify new transaction
    if (newTx.amt < 0 || newTx.time.compareTo(lastTimestamp) <= 0) return false;
    // check my balance
    if (!balanceOf.containsKey(TEAM_ACCOUNT)) balanceOf.put(TEAM_ACCOUNT, 0L);
    if (balanceOf.get(TEAM_ACCOUNT) < newTx.amt) return false;
    return true;
  }

  /** Parse zlib compressed, url-safe base64 encoded raw input. */
  private void parseInput() throws IOException {
    Base64 decoder = new Base64(true);
    byte[] decodedBytes = decoder.decode(raw_input);
    ByteArrayInputStream bais = new ByteArrayInputStream(decodedBytes);
    InflaterInputStream iis = new InflaterInputStream(bais);

    StringBuilder sb = new StringBuilder();
    byte[] buf = new byte[5];
    int rlen;
    while ((rlen = iis.read(buf)) != -1) {
      sb.append(new String(Arrays.copyOf(buf, rlen)));
    }

    Any request = JsonIterator.deserialize(sb.toString());
    newTx = request.get("new_tx").as(Q1Transaction.class);
    chain = request.get("chain").as(new TypeLiteral<List<Q1Block>>() {});
  }

  static String ccHash(String value) {
    return DigestUtils.sha256Hex(value).substring(0, 8);
  }
}
