/**
 * Copyright (c) 2020 YCSB contributors. All rights reserved.
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
 * <p>
 * ZooKeeper client binding for YCSB.
 * <p>
 */

package site.ycsb.db.zookeeper;

import java.io.IOException;
import java.nio.charset.Charset;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import site.ycsb.ByteIterator;
import site.ycsb.Client;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import site.ycsb.measurements.Measurements;
import site.ycsb.workloads.CoreWorkload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * YCSB binding for <a href="https://zookeeper.apache.org/">ZooKeeper</a>.
 *
 * See {@code zookeeper/README.md} for details.
 */
public class ZKClient extends DB {

  private ZooKeeper zk;
  private Watcher watcher;

  // Open-loop mode: requests are issued with ZooKeeper's async API and measured on completion,
  // so the sending thread never waits for a response.
  private boolean async;
  private long drainTimeoutMs;
  private Measurements measurements;
  private final AtomicLong outstanding = new AtomicLong();
  private final Object drainLock = new Object();

  private static final String CONNECT_STRING = "zookeeper.connectString";
  private static final String DEFAULT_CONNECT_STRING = "127.0.0.1:2181";
  private static final String SESSION_TIMEOUT_PROPERTY = "zookeeper.sessionTimeout";
  private static final long DEFAULT_SESSION_TIMEOUT = TimeUnit.SECONDS.toMillis(30L);
  private static final String WATCH_FLAG = "zookeeper.watchFlag";
  private static final String ASYNC_PROPERTY = "zookeeper.async";
  private static final String DRAIN_TIMEOUT_PROPERTY = "zookeeper.async.drainTimeoutMs";
  private static final long DEFAULT_DRAIN_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(10L);
  // Under overload every in-flight request can fail at once; log a sample, not each one.
  private static final long ERROR_LOG_FIRST = 10;
  private static final long ERROR_LOG_EVERY = 10000;
  private static final AtomicLong ASYNC_ERRORS = new AtomicLong();
  private static final String CONNECT_TIMEOUT_PROPERTY = "zookeeper.async.connectTimeoutMs";
  private static final long DEFAULT_CONNECT_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(5L);
  // Async mode: every client thread waits here until all threads' sessions are connected, so
  // the open-loop schedules start together and no request is queued behind a session handshake
  // (a handshake that fails would drop those requests with CONNECTIONLOSS).
  private static CountDownLatch allConnected;
  // Async mode: every thread waits here after draining its own requests and before closing its
  // session. A session close is a write through the leader; closing while other threads still have
  // requests in flight would queue those requests behind the closes and inflate the tail.
  private static CountDownLatch allDrained;
  private int threadCount;

  private static final Charset UTF_8 = Charset.forName("UTF-8");
  private static final Logger LOG = LoggerFactory.getLogger(ZKClient.class);

  public void init() throws DBException {
    Properties props = getProperties();

    String connectString = props.getProperty(CONNECT_STRING);
    if (connectString == null || connectString.length() == 0) {
      connectString = DEFAULT_CONNECT_STRING;
    }

    if(Boolean.parseBoolean(props.getProperty(WATCH_FLAG))) {
      watcher = new SimpleWatcher();
    } else {
      watcher = null;
    }

    long sessionTimeout;
    String sessionTimeoutString = props.getProperty(SESSION_TIMEOUT_PROPERTY);
    if (sessionTimeoutString != null) {
      sessionTimeout = Integer.parseInt(sessionTimeoutString);
    } else {
      sessionTimeout = DEFAULT_SESSION_TIMEOUT;
    }

    async = Boolean.parseBoolean(props.getProperty(ASYNC_PROPERTY, "false"));
    if (async) {
      if (Integer.parseInt(props.getProperty(Client.TARGET_PROPERTY, "0")) <= 0) {
        throw new DBException(ASYNC_PROPERTY + " requires -target (otherwise requests are issued unthrottled).");
      }
      if (Double.parseDouble(props.getProperty(CoreWorkload.READMODIFYWRITE_PROPORTION_PROPERTY,
          CoreWorkload.READMODIFYWRITE_PROPORTION_PROPERTY_DEFAULT)) > 0) {
        throw new DBException(ASYNC_PROPERTY + " does not support readmodifywriteproportion > 0.");
      }
      if (Boolean.parseBoolean(props.getProperty(CoreWorkload.DATA_INTEGRITY_PROPERTY,
          CoreWorkload.DATA_INTEGRITY_PROPERTY_DEFAULT))) {
        throw new DBException(ASYNC_PROPERTY + " does not support dataintegrity=true.");
      }
      drainTimeoutMs = Long.parseLong(props.getProperty(DRAIN_TIMEOUT_PROPERTY,
          String.valueOf(DEFAULT_DRAIN_TIMEOUT_MS)));
      measurements = Measurements.getMeasurements();
    }

    if (!async) {
      try {
        zk = new ZooKeeper(connectString, (int) sessionTimeout, new SimpleWatcher());
      } catch (IOException e) {
        throw new DBException("Creating connection failed.");
      }
      return;
    }

    long connectTimeoutMs = Long.parseLong(props.getProperty(CONNECT_TIMEOUT_PROPERTY,
        String.valueOf(DEFAULT_CONNECT_TIMEOUT_MS)));
    final CountDownLatch connected = new CountDownLatch(1);
    try {
      zk = new ZooKeeper(connectString, (int) sessionTimeout, e -> {
          if (e.getState() == Watcher.Event.KeeperState.SyncConnected) {
            connected.countDown();
          }
        });
      if (!connected.await(connectTimeoutMs, TimeUnit.MILLISECONDS)) {
        throw new DBException("Session not connected within " + connectTimeoutMs + " ms.");
      }
      threadCount = Integer.parseInt(props.getProperty(Client.THREAD_COUNT_PROPERTY, "1"));
      CountDownLatch all = allConnectedLatch(threadCount);
      all.countDown();
      if (!all.await(connectTimeoutMs, TimeUnit.MILLISECONDS)) {
        LOG.warn("{} client threads still not connected after {} ms; starting anyway.", all.getCount(),
            connectTimeoutMs);
      }
    } catch (IOException e) {
      throw new DBException("Creating connection failed.");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new DBException("Interrupted while connecting.");
    }
  }

  private static synchronized CountDownLatch allDrainedLatch(int threads) {
    if (allDrained == null) {
      allDrained = new CountDownLatch(threads);
    }
    return allDrained;
  }

  private static synchronized CountDownLatch allConnectedLatch(int threads) {
    if (allConnected == null) {
      allConnected = new CountDownLatch(threads);
    }
    return allConnected;
  }

  public void cleanup() throws DBException {
    try {
      if (async) {
        drainOutstanding();
        CountDownLatch all = allDrainedLatch(threadCount);
        all.countDown();
        if (!all.await(drainTimeoutMs, TimeUnit.MILLISECONDS)) {
          LOG.warn("{} client threads still draining after {} ms; closing anyway.", all.getCount(), drainTimeoutMs);
        }
      }
      zk.close();
    } catch (InterruptedException e) {
      throw new DBException("Closing connection failed.");
    }
  }

  @Override
  public void drain() throws DBException {
    if (!async) {
      return;
    }
    try {
      drainOutstanding();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new DBException("Interrupted while draining.");
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
                     Map<String, ByteIterator> result) {
    String path = getPath(key);
    if (async) {
      Pending p = new Pending("READ");
      // The caller's result map is not filled in: the call has already returned when the data arrives.
      zk.getData(path, watcher, (rc, pth, ctx, data, stat) ->
          p.complete(toStatus(rc, pth, table, data == null || data.length == 0)), null);
      return Status.PENDING;
    }
    try {
      byte[] data = zk.getData(path, watcher, null);
      if (data == null || data.length == 0) {
        return Status.NOT_FOUND;
      }

      deserializeValues(data, fields, result);
      return Status.OK;
    } catch (KeeperException | InterruptedException e) {
      LOG.error("Error when reading a path:{},tableName:{}", path, table, e);
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String table, String key,
                       Map<String, ByteIterator> values) {
    String path = getPath(key);
    String data = getJsonStrFromByteMap(values);
    if (async) {
      Pending p = new Pending("INSERT");
      zk.create(path, data.getBytes(UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,
          (rc, pth, ctx, name) -> p.complete(rc == KeeperException.Code.NODEEXISTS.intValue() ?
              Status.OK : toStatus(rc, pth, table, false)), null);
      return Status.PENDING;
    }
    try {
      zk.create(path, data.getBytes(UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT);
      return Status.OK;
    } catch (KeeperException.NodeExistsException e1) {
      return Status.OK;
    } catch (KeeperException | InterruptedException e2) {
      LOG.error("Error when inserting a path:{},tableName:{}", path, table, e2);
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {

    String path = getPath(key);
    if (async) {
      Pending p = new Pending("DELETE");
      zk.delete(path, -1, (rc, pth, ctx) -> p.complete(toStatus(rc, pth, table, false)), null);
      return Status.PENDING;
    }
    try {
      zk.delete(path, -1);
      return Status.OK;
    } catch (InterruptedException | KeeperException e) {
      LOG.error("Error when deleting a path:{},tableName:{}", path, table, e);
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String table, String key,
                       Map<String, ByteIterator> values) {
    String path = getPath(key);
    if (async) {
      updateAsync(table, path, StringByteIterator.getStringMap(values));
      return Status.PENDING;
    }
    try {
      // we have to do a read operation here before setData to meet the YCSB's update semantics:
      // update a single record in the database, adding or replacing the specified fields.
      byte[] data = zk.getData(path, watcher, null);
      if (data == null || data.length == 0) {
        return Status.NOT_FOUND;
      }
      final Map<String, ByteIterator> result = new HashMap<>();
      deserializeValues(data, null, result);
      result.putAll(values);
      // update
      zk.setData(path, getJsonStrFromByteMap(result).getBytes(UTF_8), -1);
      return Status.OK;
    } catch (KeeperException | InterruptedException e) {
      LOG.error("Error when updating a path:{},tableName:{}", path, table, e);
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }

  /**
   * Same read-then-setData semantics as the synchronous update, chained through callbacks.
   */
  private void updateAsync(String table, String path, Map<String, String> values) {
    Pending p = new Pending("UPDATE");
    zk.getData(path, watcher, (rc, pth, ctx, data, stat) -> setDataAfterRead(p, table, values, rc, pth, data), null);
  }

  private void setDataAfterRead(Pending p, String table, Map<String, String> values,
                                int rc, String path, byte[] data) {
    Status readStatus = toStatus(rc, path, table, data == null || data.length == 0);
    if (!readStatus.isOk()) {
      p.complete(readStatus);
      return;
    }
    try {
      final Map<String, ByteIterator> result = new HashMap<>();
      deserializeValues(data, null, result);
      Map<String, String> merged = StringByteIterator.getStringMap(result);
      merged.putAll(values);
      zk.setData(path, JSONValue.toJSONString(merged).getBytes(UTF_8), -1,
          (rc2, pth, ctx, stat) -> p.complete(toStatus(rc2, pth, table, false)), null);
    } catch (RuntimeException e) {
      LOG.error("Error when updating a path:{},tableName:{}", path, table, e);
      p.complete(Status.ERROR);
    }
  }

  private static Status toStatus(int rc, String path, String table, boolean empty) {
    KeeperException.Code code = KeeperException.Code.get(rc);
    if (code == KeeperException.Code.OK) {
      return empty ? Status.NOT_FOUND : Status.OK;
    }
    if (code == KeeperException.Code.NONODE) {
      return Status.NOT_FOUND;
    }
    long n = ASYNC_ERRORS.incrementAndGet();
    if (n <= ERROR_LOG_FIRST || n % ERROR_LOG_EVERY == 0) {
      LOG.error("Async error #{}: {} on path:{},tableName:{}", n, code, path, table);
    }
    return Status.ERROR;
  }

  /**
   * Waits (bounded by zookeeper.async.drainTimeoutMs) for in-flight async requests so they are measured.
   */
  private void drainOutstanding() throws InterruptedException {
    long deadline = System.currentTimeMillis() + drainTimeoutMs;
    synchronized (drainLock) {
      long left = deadline - System.currentTimeMillis();
      while (outstanding.get() > 0 && left > 0) {
        drainLock.wait(left);
        left = deadline - System.currentTimeMillis();
      }
    }
    long remaining = outstanding.get();
    if (remaining > 0) {
      LOG.warn("{} async requests still outstanding after {} ms; they are not measured.", remaining, drainTimeoutMs);
    }
  }

  /**
   * An in-flight async request. Must be created on the issuing client thread, which holds the intended start time.
   */
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
      String name = status.isOk() ? op : op + "-FAILED";
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

  private String getPath(String key) {
    return key.startsWith("/") ? key : "/" + key;
  }

  /**
   * converting the key:values map to JSON Strings.
   */
  private static String getJsonStrFromByteMap(Map<String, ByteIterator> map) {
    Map<String, String> stringMap = StringByteIterator.getStringMap(map);
    return JSONValue.toJSONString(stringMap);
  }

  private Map<String, ByteIterator> deserializeValues(final byte[] data, final Set<String> fields,
                                                      final Map<String, ByteIterator> result) {
    JSONObject jsonObject = (JSONObject)JSONValue.parse(new String(data, UTF_8));
    Iterator<String> iterator = jsonObject.keySet().iterator();
    while(iterator.hasNext()) {
      String field = iterator.next();
      String value = jsonObject.get(field).toString();
      if(fields == null || fields.contains(field)) {
        result.put(field, new StringByteIterator(value));
      }
    }
    return result;
  }

  private static class SimpleWatcher implements Watcher {

    public void process(WatchedEvent e) {
      if (e.getType() == Event.EventType.None) {
        return;
      }

      if (e.getState() == Event.KeeperState.SyncConnected) {
        //do nothing
      }
    }
  }
}
