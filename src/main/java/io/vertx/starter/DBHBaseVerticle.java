package io.vertx.starter;

import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

public class DBHBaseVerticle extends DBVerticle {
  static final String queueName = "hbase.queue";
  /** set the private IP address(es) of HBase zookeeper nodes. */
  private final String zkPrivateIPs = "172.31.17.44";
  /** The name of your HBase table. */
  private final TableName tableName = TableName.valueOf("twitter");
  /** HTable handler. */
  private Table bizTable;
  /** HBase connection. */
  private Connection conn;
  /** Byte representation of column family. */
  private final byte[] q2BColFamily = Bytes.toBytes("q2");
  private final byte[] q3BColFamily = Bytes.toBytes("q3");

  @Override
  public void start(Promise<Void> promise) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", zkPrivateIPs);
    conf.set("hbase.zookeeper.property.clientport", "2181");
    conf.set("hbase.cluster.distributed", "true");
    conf.set("zookeeper.znode.parent", "/hbase-unsecure");

    try {
      conn = ConnectionFactory.createConnection(conf);
      bizTable = conn.getTable(tableName);
    } catch (Exception e) {
      e.printStackTrace();
    }
    promise.complete();
  }

  @Override
  void fetchQ2(Message<JsonObject> message) {
    JsonObject request = message.body();
    long reqUser = request.getLong("user");

    byte[] descriptionCol = Bytes.toBytes("description");
    byte[] screenNameCol = Bytes.toBytes("screen_name");
    byte[] hashtagsCol = Bytes.toBytes("hashtags");
    byte[] recordsCol = Bytes.toBytes("records");

    Get get = new Get(Bytes.toBytes(String.valueOf(reqUser)));
    get.addColumn(q2BColFamily, descriptionCol);
    get.addColumn(q2BColFamily, screenNameCol);
    get.addColumn(q2BColFamily, hashtagsCol);
    get.addColumn(q2BColFamily, recordsCol);

    try {
      Result rs = bizTable.get(get);
      JsonObject result = new JsonObject();
      result.put("description", rs.getValue(q2BColFamily, descriptionCol));
      result.put("screen_name", rs.getValue(q2BColFamily,screenNameCol));
      result.put("hashtags", rs.getValue(q2BColFamily, hashtagsCol));
      result.put("records", rs.getValue(q2BColFamily, recordsCol));
      message.reply(result);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  void fetchQ3Words(Message<JsonObject> message) {
    JsonObject request = message.body();
  }

  @Override
  void fetchQ3Texts(Message<JsonObject> message) {
    JsonObject request = message.body();
  }
}
