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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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

    private static final int LOAD_NUM = 500_000;

    /** */
    private boolean useCache;

    /** */
    private final String[] vals = new String[2048];

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
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        for (int i = 0; i < vals.length; i++) {
            int valLen = ThreadLocalRandom.current().nextInt(128, 512);

            StringBuilder sb = new StringBuilder();

            for (int j = 0; j < valLen; j++)
                sb.append('a' + ThreadLocalRandom.current().nextInt(20));

            vals[i] = sb.toString();
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
        try {
            useCache = true;

            startGridsMultiThreaded(GRID_CNT);

            useCache = false;




            final LongAdder cnt = new LongAdder();

            System.gc();
            System.gc();
            System.gc();

//            Thread t = new Thread(new Runnable() {
//                @SuppressWarnings("BusyWait")
//                @Override public void run() {
//                    AtomicLong cacheSize = new AtomicLong(grid(0).cache(DEFAULT_CACHE_NAME).size());
//
//                    int seconds = 3;
//
//                    while (true) {
//                        try {
//                            Thread.sleep(seconds * 1000);
//                        }
//                        catch (InterruptedException ignored) {
//                            break;
//                        }
//
//                        long grow = (grow = grid(0).cache(DEFAULT_CACHE_NAME).size()) - cacheSize.getAndSet(grow);
//
//                        info(">>> Adds/sec: " + grow / seconds);
//                    }
//                }
//            });
//
//            t.setDaemon(true);
//
//            t.start();

            int threadNum = 2; //Runtime.getRuntime().availableProcessors();

            AtomicInteger loadNum = new AtomicInteger(LOAD_NUM);

            try (Ignite ignite = startClientGrid(GRID_CNT)){
                try(IgniteDataStreamer<Integer, String> ldr = ignite.dataStreamer(DEFAULT_CACHE_NAME);){
                    ldr.receiver(DataStreamerCacheUpdaters.<Integer, String>batchedSorted());
                    ldr.autoFlushFrequency(0);


                }
            }


            int i;

            while ((i = loadNum.decrementAndGet()) > 0) {
                ldr.addData(i, vals[i % vals.length]);

                cnt.increment();
            }

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
