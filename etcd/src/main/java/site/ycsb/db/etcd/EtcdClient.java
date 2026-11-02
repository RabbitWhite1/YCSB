/**
 * Copyright (c) 2026 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb.db.etcd;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.ClientBuilder;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.options.GetOption;

import java.io.File;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.measurements.Measurements;
import site.ycsb.workloads.CoreWorkload;

/**
 * YCSB binding for <a href="https://etcd.io/">etcd</a>, a port of go-ycsb's etcd binding
 * (pingcap/go-ycsb {@code db/etcd/db.go}) with the same semantics; see {@code etcd/README.md}.
 */
public class EtcdClient extends DB {

  public static final String ENDPOINTS = "etcd.endpoints";
  public static final String DIAL_TIMEOUT = "etcd.dial_timeout";
  public static final String CERT_FILE = "etcd.cert_file";
  public static final String KEY_FILE = "etcd.key_file";
  public static final String CA_FILE = "etcd.cacert_file";
  /** Open-loop mode (not in go-ycsb): issue each operation without waiting and measure it on completion. */
  public static final String ASYNC = "etcd.async";
  public static final String DRAIN_TIMEOUT = "etcd.async.drainTimeoutMs";
  /**
   * Clients (connections) per process, like go-ycsb's redis.pool_size (not in go-ycsb's etcd binding): 0, the
   * default, shares one client among all threads as go-ycsb's etcd binding does; N > 0 opens N clients, each its own
   * gRPC connection, and gives them to the threads round-robin (N = threadcount: one connection per thread).
   */
  public static final String POOL_SIZE = "etcd.pool_size";

  private static final String DEFAULT_ENDPOINTS = "localhost:2379";
  private static final long DEFAULT_DIAL_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(2);
  private static final long DEFAULT_DRAIN_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(10);
  // WithFromKey: every key >= the start key.
  private static final ByteSequence FROM_KEY_END = ByteSequence.from(new byte[] {0});
  private static final long ERROR_LOG_FIRST = 10;
  private static final long ERROR_LOG_EVERY = 10000;
  private static final AtomicLong ERRORS = new AtomicLong();
  private static final Logger LOG = LoggerFactory.getLogger(EtcdClient.class);

  // go-ycsb creates one clientv3.Client for all its threads; YCSB creates one DB per thread, so they share
  // one (or, with etcd.pool_size, a pool of clients, one per connection).
  private static List<Client> pool;
  private static int sharedRefs;
  private static int nextClient;
  private static ExecutorService poolExecutor;

  private KV kv;
  private boolean async;
  private long drainTimeoutMs;
  private Measurements measurements;
  private final AtomicLong outstanding = new AtomicLong();
  private final Object drainLock = new Object();

  @Override
  public void init() throws DBException {
    Properties props = getProperties();
    async = Boolean.parseBoolean(props.getProperty(ASYNC, "false"));
    if (async) {
      if (Integer.parseInt(props.getProperty(site.ycsb.Client.TARGET_PROPERTY, "0")) <= 0) {
        throw new DBException(ASYNC + " requires -target (otherwise requests are issued unthrottled).");
      }
      if (Double.parseDouble(props.getProperty(CoreWorkload.READMODIFYWRITE_PROPORTION_PROPERTY,
          CoreWorkload.READMODIFYWRITE_PROPORTION_PROPERTY_DEFAULT)) > 0) {
        throw new DBException(ASYNC + " does not support readmodifywriteproportion > 0.");
      }
      if (Boolean.parseBoolean(props.getProperty(CoreWorkload.DATA_INTEGRITY_PROPERTY,
          CoreWorkload.DATA_INTEGRITY_PROPERTY_DEFAULT))) {
        throw new DBException(ASYNC + " does not support dataintegrity=true.");
      }
      drainTimeoutMs = Long.parseLong(props.getProperty(DRAIN_TIMEOUT, String.valueOf(DEFAULT_DRAIN_TIMEOUT_MS)));
      measurements = Measurements.getMeasurements();
    }
    kv = acquireClient(props, async).getKVClient();
  }

  private static synchronized Client acquireClient(Properties props, boolean warmUp) throws DBException {
    if (pool == null) {
      int size = Integer.parseInt(props.getProperty(POOL_SIZE, "0"));
      List<Client> clients = new ArrayList<>();
      if (size > 0) {
        // One callback executor for the whole pool instead of one per client.
        poolExecutor = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "etcd-pool-callback");
            t.setDaemon(true);
            return t;
          });
      }
      for (int i = 0; i < Math.max(size, 1); i++) {
        clients.add(buildClient(props, poolExecutor));
      }
      if (warmUp) {
        // Async mode only: connect before the open-loop schedule starts, so no measured request waits for
        // the connection. go-ycsb (and the synchronous mode here) connect on the first operation.
        for (Client c : clients) {
          try {
            c.getClusterClient().listMember().get();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DBException("Interrupted while connecting.");
          } catch (ExecutionException e) {
            throw new DBException("Cannot reach etcd: " + e.getCause());
          }
        }
      }
      pool = clients;
    }
    sharedRefs++;
    return pool.get(nextClient++ % pool.size());
  }

  /** clientv3.Config of go-ycsb's getClientConfig, on jetcd. */
  static Client buildClient(Properties props, ExecutorService executor) throws DBException {
    String endpoints = props.getProperty(ENDPOINTS, DEFAULT_ENDPOINTS);
    boolean tls = endpoints.contains("https");
    ClientBuilder b = Client.builder()
        .endpoints(endpointUris(endpoints, tls))
        // clientv3's resolver sets {"loadBalancingPolicy":"round_robin"} and clientv3 calls wait for ready.
        .loadBalancerPolicy("round_robin")
        .waitForReady(true)
        .connectTimeout(Duration.ofNanos(dialTimeoutNs(props.getProperty(DIAL_TIMEOUT))));
    if (executor != null) {
      b.executorService(executor);
    }
    if (tls) {
      // go-ycsb: MustGetString for all three, then transport.TLSInfo{...}.ClientConfig().
      String cert = required(props, CERT_FILE);
      String key = required(props, KEY_FILE);
      String ca = required(props, CA_FILE);
      try {
        b.sslContext(s -> s.trustManager(new File(ca)).keyManager(new File(cert), new File(key)));
      } catch (javax.net.ssl.SSLException e) {
        throw new DBException("TLS setup failed: " + e);
      }
    }
    return b.build();
  }

  /** go-ycsb splits etcd.endpoints on ","; clientv3 takes host:port without a scheme, jetcd needs a URI. */
  static URI[] endpointUris(String endpoints, boolean tls) {
    String[] parts = endpoints.split(",", -1);
    URI[] uris = new URI[parts.length];
    for (int i = 0; i < parts.length; i++) {
      String e = parts[i];
      if (!e.contains("://")) {
        if (tls) {
          e = "https://" + e;
        } else {
          e = "http://" + e;
        }
      }
      uris[i] = URI.create(e);
    }
    return uris;
  }

  /** magiconair/properties GetDuration: an integer number of nanoseconds, else the default. */
  static long dialTimeoutNs(String value) {
    if (value == null) {
      return DEFAULT_DIAL_TIMEOUT_NS;
    }
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      return DEFAULT_DIAL_TIMEOUT_NS;
    }
  }

  private static String required(Properties props, String name) throws DBException {
    String v = props.getProperty(name);
    if (v == null) {
      throw new DBException("Missing property " + name + " (required for https endpoints).");
    }
    return v;
  }

  @Override
  public void cleanup() throws DBException {
    if (async) {
      drainOutstanding();
    }
    synchronized (EtcdClient.class) {
      if (--sharedRefs == 0) {
        for (Client c : pool) {
          c.close();
        }
        pool = null;
        nextClient = 0;
        if (poolExecutor != null) {
          poolExecutor.shutdown();
          poolExecutor = null;
        }
      }
    }
  }

  @Override
  public void drain() throws DBException {
    if (async) {
      drainOutstanding();
    }
  }

  /** go-ycsb getRowKey: table + ":" + key. */
  static ByteSequence rowKey(String table, String key) {
    return ByteSequence.from(table + ":" + key, StandardCharsets.UTF_8);
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    CompletableFuture<GetResponse> f = kv.get(rowKey(table, key));
    if (async) {
      Pending p = new Pending("READ");
      f.whenComplete((r, e) -> p.complete(readStatus(table, key, r, e, null)));
      return Status.PENDING;
    }
    try {
      return readStatus(table, key, f.get(), null, result);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return error("READ", table, key, e);
    } catch (ExecutionException e) {
      return error("READ", table, key, e.getCause());
    }
  }

  /**
   * go-ycsb Read: an error if the key is missing (not NOT_FOUND), otherwise the decoded record. The
   * requested fields are ignored; every stored field is returned.
   */
  private static Status readStatus(String table, String key, GetResponse r, Throwable e,
                                   Map<String, ByteIterator> result) {
    if (r == null) {
      return error("READ", table, key, e);
    }
    if (r.getCount() == 0) {
      return error("READ", table, key, new IllegalStateException("could not find value for key"));
    }
    try {
      Map<String, byte[]> record = GoJson.decode(r.getKvs().get(0).getValue().getBytes());
      if (result != null) {
        putAll(result, record);
      }
      return Status.OK;
    } catch (IllegalArgumentException ex) {
      return error("READ", table, key, ex);
    }
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    GetOption opt = GetOption.builder().withRange(FROM_KEY_END).withLimit(recordcount).build();
    CompletableFuture<GetResponse> f = kv.get(rowKey(table, startkey), opt);
    if (async) {
      Pending p = new Pending("SCAN");
      f.whenComplete((r, e) -> p.complete(scanStatus(table, startkey, recordcount, r, e, null)));
      return Status.PENDING;
    }
    try {
      return scanStatus(table, startkey, recordcount, f.get(), null, result);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return error("SCAN", table, startkey, e);
    } catch (ExecutionException e) {
      return error("SCAN", table, startkey, e.getCause());
    }
  }

  /**
   * go-ycsb Scan, including its quirks: the response's Count (all keys from the start key on, not just the
   * returned ones) must equal recordcount, and the result holds recordcount empty entries before the records.
   */
  private static Status scanStatus(String table, String startkey, int recordcount, GetResponse r, Throwable e,
                                   Vector<HashMap<String, ByteIterator>> result) {
    if (r == null) {
      return error("SCAN", table, startkey, e);
    }
    if (r.getCount() != recordcount) {
      return error("SCAN", table, startkey, new IllegalStateException(
          "unexpected number of result, expected " + recordcount + " but was " + r.getCount()));
    }
    Vector<HashMap<String, ByteIterator>> rows = new Vector<>();
    for (int i = 0; i < recordcount; i++) {
      rows.add(null);
    }
    try {
      for (KeyValue v : r.getKvs()) {
        HashMap<String, ByteIterator> row = new HashMap<>();
        putAll(row, GoJson.decode(v.getValue().getBytes()));
        rows.add(row);
      }
    } catch (IllegalArgumentException ex) {
      return error("SCAN", table, startkey, ex);
    }
    if (result != null) {
      result.addAll(rows);
    }
    return Status.OK;
  }

  /** go-ycsb Update: Put of only the given fields, replacing the whole stored record. */
  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    return put("UPDATE", table, key, values);
  }

  /** go-ycsb Insert is its Update. */
  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    return put("INSERT", table, key, values);
  }

  private Status put(String op, String table, String key, Map<String, ByteIterator> values) {
    Map<String, byte[]> record = new HashMap<>();
    for (Map.Entry<String, ByteIterator> v : values.entrySet()) {
      record.put(v.getKey(), v.getValue().toArray());
    }
    CompletableFuture<?> f = kv.put(rowKey(table, key), ByteSequence.from(GoJson.encode(record)));
    return finish(op, table, key, f);
  }

  /** go-ycsb Delete: OK whether or not the key existed. */
  @Override
  public Status delete(String table, String key) {
    return finish("DELETE", table, key, kv.delete(rowKey(table, key)));
  }

  private Status finish(String op, String table, String key, CompletableFuture<?> f) {
    if (async) {
      Pending p = new Pending(op);
      f.whenComplete((r, e) -> {
          if (e == null) {
            p.complete(Status.OK);
          } else {
            p.complete(error(op, table, key, e));
          }
        });
      return Status.PENDING;
    }
    try {
      f.get();
      return Status.OK;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return error(op, table, key, e);
    } catch (ExecutionException e) {
      return error(op, table, key, e.getCause());
    }
  }

  private static void putAll(Map<String, ByteIterator> out, Map<String, byte[]> record) {
    for (Map.Entry<String, byte[]> f : record.entrySet()) {
      byte[] v = f.getValue();
      if (v == null) {
        v = new byte[0];
      }
      out.put(f.getKey(), new ByteArrayByteIterator(v));
    }
  }

  private static Status error(String op, String table, String key, Throwable e) {
    long n = ERRORS.incrementAndGet();
    if (n <= ERROR_LOG_FIRST || n % ERROR_LOG_EVERY == 0) {
      LOG.error("etcd error #{}: {} {}:{}: {}", n, op, table, key, String.valueOf(e));
    }
    return Status.ERROR;
  }

  /** Waits (bounded by etcd.async.drainTimeoutMs) for this thread's in-flight requests so they are measured. */
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
      LOG.warn("{} async requests still outstanding after {} ms; they are not measured.", remaining, drainTimeoutMs);
    }
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
