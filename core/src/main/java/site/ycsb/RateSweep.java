/**
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

package site.ycsb;

import site.ycsb.measurements.Measurements;
import site.ycsb.measurements.OneMeasurementRaw;

import java.util.Properties;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

/**
 * Runs the transaction phase as a series of stages, one per target rate, in a single process.
 *
 * <p>Every stage runs {@code ratesweep.operationcount} operations at its rate. At the end of a stage
 * each client thread drains its in-flight operations ({@link DB#drain()}) and waits at a barrier.
 * When all threads are there, the stage's measurements are exported and reset, the client sleeps
 * {@code ratesweep.settle.ms}, and the next stage starts for all threads at the same instant.
 * So no operation of one stage is measured in, or competes with, the next one.
 */
public final class RateSweep {
  public static final String RATES_PROPERTY = "ratesweep";
  public static final String OPERATION_COUNT_PROPERTY = "ratesweep.operationcount";
  public static final String SETTLE_MS_PROPERTY = "ratesweep.settle.ms";
  public static final String SETTLE_MS_DEFAULT = "5000";

  /** Exports one finished stage; called with every client thread parked at the barrier. */
  public interface StageExporter {
    void export(int stage, int rate, long ops, long runtimeMs) throws Exception;
  }

  private final int[] rates;
  private final int stageOps;
  private final long settleMs;
  private final Properties baseProps;
  private final CyclicBarrier barrier;
  private final StageExporter exporter;
  private final ClientThread[] clients;

  private int stage = -1;
  private volatile long stageStartNs;
  private long stageStartMs;
  private long opsBeforeStage;

  /** Returns null when {@code ratesweep} is not set. */
  public static int[] parseRates(Properties props) {
    String s = props.getProperty(RATES_PROPERTY);
    if (s == null || s.trim().isEmpty()) {
      return null;
    }
    String[] parts = s.split(",");
    int[] rates = new int[parts.length];
    for (int i = 0; i < parts.length; i++) {
      rates[i] = Integer.parseInt(parts[i].trim());
      if (rates[i] <= 0) {
        throw new IllegalArgumentException(RATES_PROPERTY + " rates must be > 0: " + s);
      }
    }
    return rates;
  }

  /** Properties for a stage: the raw per-op output file gets a {@code .<rate>} suffix. */
  public static Properties stageProps(Properties props, int rate) {
    String raw = props.getProperty(OneMeasurementRaw.OUTPUT_FILE_PATH, "");
    if (raw.isEmpty()) {
      return props;
    }
    Properties p = new Properties();
    p.putAll(props);
    p.setProperty(OneMeasurementRaw.OUTPUT_FILE_PATH, raw + "." + rate);
    return p;
  }

  RateSweep(int[] rates, int stageOps, Properties props, int threadcount, ClientThread[] clients,
            StageExporter exporter) {
    this.rates = rates;
    this.stageOps = stageOps;
    this.settleMs = Long.parseLong(props.getProperty(SETTLE_MS_PROPERTY, SETTLE_MS_DEFAULT));
    this.baseProps = props;
    this.exporter = exporter;
    this.clients = clients;
    this.barrier = new CyclicBarrier(threadcount, this::onAllArrived);
  }

  int stages() {
    return rates.length;
  }

  int rate(int s) {
    return rates[s];
  }

  /** Operations thread {@code threadid} of {@code threadcount} runs in each stage. */
  int threadOps(int threadid, int threadcount) {
    int n = stageOps / threadcount;
    return threadid < stageOps % threadcount ? n + 1 : n;
  }

  long stageStartNs() {
    return stageStartNs;
  }

  /** Blocks until every client thread has finished (and drained) the current stage. */
  void await() throws InterruptedException, BrokenBarrierException {
    barrier.await();
  }

  /** Barrier action: runs in the last arriving thread while all others wait. */
  private void onAllArrived() {
    long totalOps = 0;
    for (ClientThread c : clients) {
      totalOps += c.getOpsDone();
    }
    if (stage >= 0) {
      long runtimeMs = System.currentTimeMillis() - stageStartMs;
      try {
        exporter.export(stage, rates[stage], totalOps - opsBeforeStage, runtimeMs);
      } catch (Exception e) {
        System.err.println("Could not export rate-sweep stage " + stage + ": " + e);
        e.printStackTrace();
      }
      if (stage + 1 < rates.length) {
        Measurements.getMeasurements().reset(stageProps(baseProps, rates[stage + 1]));
        System.err.println("[RATESWEEP] settling " + settleMs + " ms before " + rates[stage + 1] + " ops/s");
        try {
          Thread.sleep(settleMs);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }
    stage++;
    opsBeforeStage = totalOps;
    if (stage < rates.length) {
      System.err.println("[RATESWEEP] stage " + stage + ": " + rates[stage] + " ops/s, " + stageOps + " ops");
    }
    stageStartMs = System.currentTimeMillis();
    stageStartNs = System.nanoTime();
  }
}
