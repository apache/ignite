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

package org.apache.ignite.internal.processors.datastreamer;

import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.LongAdder;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.events.EventType.EVT_JOB_MAPPED;
import static org.apache.ignite.events.EventType.EVT_TASK_FAILED;
import static org.apache.ignite.events.EventType.EVT_TASK_FINISHED;

/**
 * Data streamer performance test. Compares group lock data streamer to traditional lock.
 * <p>
 * Disable assertions and give at least 2 GB heap to run this test.
 */
public class IgniteDataStreamerPerformanceTest extends GridCommonAbstractTest {
    /** */
    private static final int GRID_CNT = 3;

    /** */
    private static final int ENTRY_CNT = 80000;

    /** */
    private boolean useCache;

    /** */
    private String[] vals = new String[2048];

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setIncludeProperties();

        cfg.setIncludeEventTypes(EVT_TASK_FAILED, EVT_TASK_FINISHED, EVT_JOB_MAPPED);

        cfg.setConnectorConfiguration(null);

        cfg.setPeerClassLoadingEnabled(true);

        if (useCache) {
            CacheConfiguration cc = defaultCacheConfiguration();

            cc.setCacheMode(PARTITIONED);

            cc.setNearConfiguration(null);
            cc.setWriteSynchronizationMode(FULL_SYNC);

            cc.setBackups(1);

            cfg.setCacheSanityCheckEnabled(false);
            cfg.setCacheConfiguration(cc);
        }
        else
            cfg.setCacheConfiguration();

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        for (int i = 0; i < vals.length; i++) {
            int valLen = ThreadLocalRandom.current().nextInt(128, 512);

            StringBuilder sb = new StringBuilder();

            for (int j = 0; j < valLen; j++)
                sb.append('a' + ThreadLocalRandom.current().nextInt(20));

            vals[i] = sb.toString();

            info("Value: " + vals[i]);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPerformance() throws Exception {
        doTest();
    }

    /**
     * @throws Exception If failed.
     */
    private void doTest() throws Exception {
        System.gc();
        System.gc();
        System.gc();

        try {
            useCache = true;

            startGridsMultiThreaded(GRID_CNT);

            useCache = false;

            Ignite ignite = startGrid();

            final IgniteDataStreamer<Integer, String> ldr = ignite.dataStreamer(DEFAULT_CACHE_NAME);

            ldr.perNodeBufferSize(8192);
            ldr.receiver(DataStreamerCacheUpdaters.<Integer, String>batchedSorted());
            ldr.autoFlushFrequency(0);

            final LongAdder cnt = new LongAdder();

            long start = U.currentTimeMillis();

            Thread t = new Thread(new Runnable() {
                @SuppressWarnings("BusyWait")
                @Override public void run() {
                    while (true) {
                        try {
                            Thread.sleep(10000);
                        }
                        catch (InterruptedException ignored) {
                            break;
                        }

                        info(">>> Adds/sec: " + cnt.sumThenReset() / 10);
                    }
                }
            });

            t.setDaemon(true);

            t.start();

            int threadNum = 2;//Runtime.getRuntime().availableProcessors();

            multithreaded(new Callable<Object>() {
                @SuppressWarnings("InfiniteLoopStatement")
                @Override public Object call() throws Exception {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    while (true) {
                        int i = rnd.nextInt(ENTRY_CNT);

                        ldr.addData(i, vals[rnd.nextInt(vals.length)]);

                        cnt.increment();
                    }
                }
            }, threadNum, "loader");

            info("Closing loader...");

            ldr.close(false);

            long duration = U.currentTimeMillis() - start;

            info("Finished performance test. Duration: " + duration + "ms.");
        }
        finally {
            stopAllGrids();
        }
    }
}
