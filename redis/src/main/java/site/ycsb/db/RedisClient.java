/**
 * Copyright (c) 2012 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

/**
 * Redis client binding for YCSB.
 *
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 *
 * With redis.async=true (open-loop mode, requires -target) the same commands are issued through
 * Lettuce's asynchronous API on one connection per client thread, and each operation is measured on
 * completion from its intended start time, like zookeeper.async.
 */

package site.ycsb.db;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisURI;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import site.ycsb.ByteIterator;
import site.ycsb.Client;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import site.ycsb.measurements.Measurements;
import site.ycsb.workloads.CoreWorkload;
import redis.clients.jedis.BasicCommands;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.Protocol;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.HashMap;
import java.util.Map;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

/**
 * YCSB binding for <a href="http://redis.io/">Redis</a>.
 *
 * See {@code redis/README.md} for details.
 */
public class RedisClient extends DB {

  private JedisCommands jedis;

  public static final String HOST_PROPERTY = "redis.host";
  public static final String PORT_PROPERTY = "redis.port";
  public static final String PASSWORD_PROPERTY = "redis.password";
  public static final String CLUSTER_PROPERTY = "redis.cluster";
  public static final String TIMEOUT_PROPERTY = "redis.timeout";

  public static final String INDEX_KEY = "_indices";

  public static final String ASYNC_PROPERTY = "redis.async";
  public static final String DRAIN_TIMEOUT_PROPERTY = "redis.async.drainTimeoutMs";
  // Async mode, like go-ycsb's redis.pool_size: > 0 opens that many connections per process, shared by the
  // threads round-robin (Lettuce connections are thread-safe); 0 (default): one connection per thread.
  public static final String POOL_SIZE_PROPERTY = "redis.pool_size";
  private static final long DEFAULT_DRAIN_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(10L);
  private static final long CONNECT_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(5L);
  private static final long ERROR_LOG_FIRST = 10;
  private static final long ERROR_LOG_EVERY = 10000;
  private static final AtomicLong ASYNC_ERRORS = new AtomicLong();

  // Async mode: one Lettuce client (event loops) per process, one connection per client thread.
  private static io.lettuce.core.RedisClient lettuce;
  private static int lettuceRefs;
  // Every thread waits here until all threads are connected, so the open-loop schedules start together.
  private static CountDownLatch allConnected;
  private boolean async;
  private long drainTimeoutMs;
  private Measurements measurements;
  private StatefulRedisConnection<String, String> connection;
  private static List<StatefulRedisConnection<String, String>> connectionPool;
  private static int connectionPoolRefs;
  private static int nextConnection;
  private boolean pooled;
  private RedisAsyncCommands<String, String> commands;
  private final AtomicLong outstanding = new AtomicLong();
  private final Object drainLock = new Object();

  public void init() throws DBException {
    Properties props = getProperties();
    int port;

    String portString = props.getProperty(PORT_PROPERTY);
    if (portString != null) {
      port = Integer.parseInt(portString);
    } else {
      port = Protocol.DEFAULT_PORT;
    }
    String host = props.getProperty(HOST_PROPERTY);

    async = Boolean.parseBoolean(props.getProperty(ASYNC_PROPERTY, "false"));
    if (async) {
      initAsync(props, host, port);
      return;
    }

    boolean clusterEnabled = Boolean.parseBoolean(props.getProperty(CLUSTER_PROPERTY));
    if (clusterEnabled) {
      Set<HostAndPort> jedisClusterNodes = new HashSet<>();
      jedisClusterNodes.add(new HostAndPort(host, port));
      jedis = new JedisCluster(jedisClusterNodes);
    } else {
      String redisTimeout = props.getProperty(TIMEOUT_PROPERTY);
      if (redisTimeout != null){
        jedis = new Jedis(host, port, Integer.parseInt(redisTimeout));
      } else {
        jedis = new Jedis(host, port);
      }
      ((Jedis) jedis).connect();
    }

    String password = props.getProperty(PASSWORD_PROPERTY);
    if (password != null) {
      ((BasicCommands) jedis).auth(password);
    }
  }

  private void initAsync(Properties props, String host, int port) throws DBException {
    if (Integer.parseInt(props.getProperty(Client.TARGET_PROPERTY, "0")) <= 0) {
      throw new DBException(ASYNC_PROPERTY + " requires -target (otherwise requests are issued unthrottled).");
    }
    if (Boolean.parseBoolean(props.getProperty(CLUSTER_PROPERTY))) {
      throw new DBException(ASYNC_PROPERTY + " does not support " + CLUSTER_PROPERTY + ".");
    }
    if (Double.parseDouble(props.getProperty(CoreWorkload.READMODIFYWRITE_PROPORTION_PROPERTY,
        CoreWorkload.READMODIFYWRITE_PROPORTION_PROPERTY_DEFAULT)) > 0
        || Double.parseDouble(props.getProperty(CoreWorkload.SCAN_PROPORTION_PROPERTY,
        CoreWorkload.SCAN_PROPORTION_PROPERTY_DEFAULT)) > 0) {
      throw new DBException(ASYNC_PROPERTY + " does not support read-modify-write or scan operations.");
    }
    if (Boolean.parseBoolean(props.getProperty(CoreWorkload.DATA_INTEGRITY_PROPERTY,
        CoreWorkload.DATA_INTEGRITY_PROPERTY_DEFAULT))) {
      throw new DBException(ASYNC_PROPERTY + " does not support dataintegrity=true.");
    }
    drainTimeoutMs = Long.parseLong(props.getProperty(DRAIN_TIMEOUT_PROPERTY,
        String.valueOf(DEFAULT_DRAIN_TIMEOUT_MS)));
    measurements = Measurements.getMeasurements();
    RedisURI.Builder uri = RedisURI.builder().withHost(host).withPort(port);
    String password = props.getProperty(PASSWORD_PROPERTY);
    if (password != null) {
      uri.withPassword(password.toCharArray());
    }
    int poolSize = Integer.parseInt(props.getProperty(POOL_SIZE_PROPERTY, "0"));
    try {
      io.lettuce.core.RedisClient client = acquireLettuce();
      if (poolSize > 0) {
        connection = acquirePooled(client, uri.build(), poolSize);
        pooled = true;
      } else {
        connection = client.connect(uri.build());
      }
    } catch (RuntimeException e) {
      releaseLettuce();
      throw new DBException("Creating connection failed: " + e);
    }
    commands = connection.async();
    int threads = Integer.parseInt(props.getProperty(Client.THREAD_COUNT_PROPERTY, "1"));
    CountDownLatch all = allConnectedLatch(threads);
    all.countDown();
    try {
      if (!all.await(CONNECT_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
        System.err.println(all.getCount() + " client threads still not connected; starting anyway.");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new DBException("Interrupted while connecting.");
    }
  }

  private static synchronized io.lettuce.core.RedisClient acquireLettuce() {
    if (lettuce == null) {
      lettuce = io.lettuce.core.RedisClient.create();
      // No command timeout: an open-loop run measures every request to its completion.
      lettuce.setOptions(ClientOptions.builder()
          .timeoutOptions(TimeoutOptions.builder().timeoutCommands(false).build()).build());
      lettuce.setDefaultTimeout(Duration.ofDays(1));
    }
    lettuceRefs++;
    return lettuce;
  }

  private static synchronized void releaseLettuce() {
    if (--lettuceRefs == 0) {
      lettuce.shutdown();
      lettuce = null;
    }
  }

  private static synchronized StatefulRedisConnection<String, String> acquirePooled(
      io.lettuce.core.RedisClient client, RedisURI uri, int size) {
    if (connectionPool == null) {
      List<StatefulRedisConnection<String, String>> pool = new java.util.ArrayList<>();
      for (int i = 0; i < size; i++) {
        pool.add(client.connect(uri));
      }
      connectionPool = pool;
    }
    connectionPoolRefs++;
    return connectionPool.get(nextConnection++ % connectionPool.size());
  }

  private static synchronized void releasePooled() {
    if (--connectionPoolRefs == 0) {
      for (StatefulRedisConnection<String, String> c : connectionPool) {
        c.close();
      }
      connectionPool = null;
      nextConnection = 0;
    }
  }

  private static synchronized CountDownLatch allConnectedLatch(int threads) {
    if (allConnected == null) {
      allConnected = new CountDownLatch(threads);
    }
    return allConnected;
  }

  @Override
  public void drain() throws DBException {
    if (async) {
      drainOutstanding();
    }
  }

  /** Waits (bounded by redis.async.drainTimeoutMs) for this thread's in-flight requests so they are measured. */
  private void drainOutstanding() throws DBException {
    long deadline = System.currentTimeMillis() + drainTimeoutMs;
    try {
      synchronized (drainLock) {
        long left = deadline - System.currentTimeMillis();
        while (outstanding.get() > 0 && left > 0) {
          drainLock.wait(left);
          left = deadline - System.currentTimeMillis();
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new DBException("Interrupted while draining.");
    }
    long remaining = outstanding.get();
    if (remaining > 0) {
      System.err.println(remaining + " async requests still outstanding after " + drainTimeoutMs
          + " ms; they are not measured.");
    }
  }

  public void cleanup() throws DBException {
    if (async) {
      drainOutstanding();
      if (pooled) {
        releasePooled();
      } else {
        connection.close();
      }
      releaseLettuce();
      return;
    }
    try {
      ((Closeable) jedis).close();
    } catch (IOException e) {
      throw new DBException("Closing connection failed.");
    }
  }

  /*
   * Calculate a hash for a key to store it in an index. The actual return value
   * of this function is not interesting -- it primarily needs to be fast and
   * scattered along the whole space of doubles. In a real world scenario one
   * would probably use the ASCII values of the keys.
   */
  private double hash(String key) {
    return key.hashCode();
  }

  // XXX jedis.select(int index) to switch to `table`

  @Override
  public Status read(String table, String key, Set<String> fields,
      Map<String, ByteIterator> result) {
    if (async) {
      Pending p = new Pending("READ");
      if (fields == null) {
        // The caller's result map is not filled in: the call has returned before the data arrives.
        complete(p, commands.hgetall(key), m -> m.isEmpty() ? Status.ERROR : Status.OK);
      } else {
        complete(p, commands.hmget(key, fields.toArray(new String[fields.size()])), RedisClient::hmgetStatus);
      }
      return Status.PENDING;
    }
    if (fields == null) {
      StringByteIterator.putAllAsByteIterators(result, jedis.hgetAll(key));
    } else {
      String[] fieldArray =
          (String[]) fields.toArray(new String[fields.size()]);
      List<String> values = jedis.hmget(key, fieldArray);

      Iterator<String> fieldIterator = fields.iterator();
      Iterator<String> valueIterator = values.iterator();

      while (fieldIterator.hasNext() && valueIterator.hasNext()) {
        result.put(fieldIterator.next(),
            new StringByteIterator(valueIterator.next()));
      }
      assert !fieldIterator.hasNext() && !valueIterator.hasNext();
    }
    return result.isEmpty() ? Status.ERROR : Status.OK;
  }

  @Override
  public Status insert(String table, String key,
      Map<String, ByteIterator> values) {
    if (async) {
      Pending p = new Pending("INSERT");
      complete(p, commands.hmset(key, StringByteIterator.getStringMap(values)).thenCompose(r -> {
          if (!"OK".equals(r)) {
            return java.util.concurrent.CompletableFuture.completedFuture(Status.ERROR);
          }
          return commands.zadd(INDEX_KEY, hash(key), key).thenApply(n -> Status.OK);
        }), s -> s);
      return Status.PENDING;
    }
    if (jedis.hmset(key, StringByteIterator.getStringMap(values))
        .equals("OK")) {
      jedis.zadd(INDEX_KEY, hash(key), key);
      return Status.OK;
    }
    return Status.ERROR;
  }

  @Override
  public Status delete(String table, String key) {
    if (async) {
      Pending p = new Pending("DELETE");
      complete(p, commands.del(key).thenCompose(d -> commands.zrem(INDEX_KEY, key)
          .thenApply(z -> d == 0 && z == 0 ? Status.ERROR : Status.OK)), s -> s);
      return Status.PENDING;
    }
    return jedis.del(key) == 0 && jedis.zrem(INDEX_KEY, key) == 0 ? Status.ERROR
        : Status.OK;
  }

  @Override
  public Status update(String table, String key,
      Map<String, ByteIterator> values) {
    if (async) {
      Pending p = new Pending("UPDATE");
      complete(p, commands.hmset(key, StringByteIterator.getStringMap(values)),
          r -> "OK".equals(r) ? Status.OK : Status.ERROR);
      return Status.PENDING;
    }
    return jedis.hmset(key, StringByteIterator.getStringMap(values))
        .equals("OK") ? Status.OK : Status.ERROR;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    Set<String> keys = jedis.zrangeByScore(INDEX_KEY, hash(startkey),
        Double.POSITIVE_INFINITY, 0, recordcount);

    HashMap<String, ByteIterator> values;
    for (String key : keys) {
      values = new HashMap<String, ByteIterator>();
      read(table, key, fields, values);
      result.add(values);
    }

    return Status.OK;
  }


  /** HMGET in the synchronous read: an error if no field came back. */
  private static Status hmgetStatus(List<KeyValue<String, String>> values) {
    for (KeyValue<String, String> v : values) {
      if (v.hasValue()) {
        return Status.OK;
      }
    }
    return Status.ERROR;
  }

  private <T> void complete(Pending p, CompletionStage<T> f, java.util.function.Function<T, Status> status) {
    f.whenComplete((r, e) -> {
        if (e != null) {
          long n = ASYNC_ERRORS.incrementAndGet();
          if (n <= ERROR_LOG_FIRST || n % ERROR_LOG_EVERY == 0) {
            System.err.println("Async error #" + n + " on " + p.op + ": " + e);
          }
          p.complete(Status.ERROR);
        } else {
          p.complete(status.apply(r));
        }
      });
  }

  /** An in-flight async request; created on the issuing client thread, which holds the intended start time. */
  private final class Pending {
    private final String op;
    private final long intendedStartNs;
    private final long startNs;

    Pending(String op) {
      this.op = op;
      this.intendedStartNs = measurements.getIntendedStartTimeNs();
      this.startNs = System.nanoTime();
      outstanding.incrementAndGet();
    }

    /** Mirrors DBWrapper's measurement: failures are recorded under OP-FAILED. */
    void complete(Status status) {
      long endNs = System.nanoTime();
      String name = op;
      if (!status.isOk()) {
        name = op + "-FAILED";
      }
      measurements.measure(name, (int) ((endNs - startNs) / 1000));
      measurements.measureIntended(name, (int) ((endNs - intendedStartNs) / 1000));
      measurements.reportStatus(op, status);
      if (outstanding.decrementAndGet() == 0) {
        synchronized (drainLock) {
          drainLock.notifyAll();
        }
      }
    }
  }
}
