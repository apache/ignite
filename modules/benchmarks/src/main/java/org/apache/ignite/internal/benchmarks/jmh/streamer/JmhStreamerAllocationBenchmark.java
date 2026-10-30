/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.benchmarks.jmh.streamer;

import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.internal.benchmarks.jmh.cache.JmhCacheAllocationAbstractBenchmark;
import org.apache.ignite.internal.benchmarks.model.IntValue;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.IterationParams;

/** Loads new keys with a data streamer: each iteration adds {@link #KEYS} keys the cache doesn't have. */
public class JmhStreamerAllocationBenchmark extends JmhCacheAllocationAbstractBenchmark {
    /** Keys each iteration adds. */
    private static final int KEYS = 10_000;

    /** Wait after each iteration, longer than the work that follows a flush takes. */
    private static final long TAIL_WAIT_MS = 500;

    /** Whether the streamer writes every key as a put does, or only the keys the cache doesn't have. */
    @Param("false")
    private boolean allowOverwrite;

    /** Keys the iterations add, after the ones the setup writes. */
    private Integer[] newKeys;

    /** Index of the next key in {@link #newKeys}. */
    private int next;

    /** The value of every new key. */
    private final IntValue val = new IntValue(1_000_000);

    /** Streamer of the iterations. */
    private IgniteDataStreamer<Integer, IntValue> streamer;

    /** {@inheritDoc} */
    @Override public void setup() throws Exception {
        super.setup();

        Integer[] keys = new Integer[CNT];
        IntValue[] vals = new IntValue[CNT];

        for (int i = 0; i < CNT; i++) {
            keys[i] = i;
            vals[i] = new IntValue(i);
        }

        populate(keys, vals);

        streamer = node.dataStreamer(cache.getName());
        streamer.allowOverwrite(allowOverwrite);
    }

    /**
     * Creates the keys of every iteration of the run, so that adding a key allocates no key object.
     *
     * @param params Benchmark parameters.
     */
    @Setup
    public void createNewKeys(BenchmarkParams params) {
        IterationParams w = params.getWarmup();
        IterationParams m = params.getMeasurement();
        int calls = w.getCount() * w.getBatchSize() + m.getCount() * m.getBatchSize();

        newKeys = new Integer[calls * KEYS];

        for (int i = 0; i < newKeys.length; i++)
            newKeys[i] = CNT + i;
    }

    /**
     * Waits for the client's streamer threads and the backups, which finish a batch after the flush returns.
     *
     * @throws InterruptedException If interrupted.
     */
    @TearDown(Level.Iteration)
    public void awaitTail() throws InterruptedException {
        Thread.sleep(TAIL_WAIT_MS);
    }

    /** Adds {@link #KEYS} new keys and flushes them; results are per key. */
    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @OperationsPerInvocation(KEYS)
    public void addData() {
        for (int i = 0; i < KEYS; i++)
            streamer.addData(newKeys[next++], val);

        streamer.flush();
    }
}
