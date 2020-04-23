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
package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.DataRegionMetrics;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.metric.impl.HitRateMetric;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test to visualize and debug {@link PagesWriteThrottle}. Prints puts/gets rate, number of dirty pages, pages written
 * in current checkpoint and pages in checkpoint buffer. Not intended to be part of any test suite.
 */
public class PagesWriteThrottleSandboxTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "cache1";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        DataStorageConfiguration dbCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setMaxSize(4000L * 1024 * 1024)
                .setCheckpointPageBufferSize(1000L * 1000 * 1000)
                .setName("dfltDataRegion")
                .setMetricsEnabled(true)
                .setPersistenceEnabled(true))
            .setWalMode(WALMode.BACKGROUND)
            .setCheckpointFrequency(20_000)
            .setWriteThrottlingEnabled(true);

        cfg.setDataStorageConfiguration(dbCfg);

        CacheConfiguration ccfg1 = new CacheConfiguration();

        ccfg1.setName(CACHE_NAME);
        ccfg1.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg1.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg1.setAffinity(new RendezvousAffinityFunction(false, 64));

        cfg.setCacheConfiguration(ccfg1);

        cfg.setConsistentId(gridName);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        deleteWorkFiles();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        deleteWorkFiles();
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 100 * 60 * 1000;
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testThrottle() throws Exception {
        startGrids(1).active(true);

        try {
            final Ignite ig = ignite(0);

            final int keyCnt = 4_000_000;

            final AtomicBoolean run = new AtomicBoolean(true);

            final HitRateMetric getRate = new HitRateMetric("getRate", "", 5000, 5);

            GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    while (run.get()) {
                        ThreadLocalRandom rnd = ThreadLocalRandom.current();

                        int key = rnd.nextInt(keyCnt * 2);

                        ignite(0).cache(CACHE_NAME).get(key);

                        getRate.increment();
                    }

                    return null;
                }
            }, 2, "read-loader");

            final HitRateMetric putRate = new HitRateMetric("putRate", "", 1000, 5);

            GridTestUtils.runAsync(new Runnable() {
                @Override public void run() {
                    while (run.get()) {
                        long dirtyPages = 0;

                        for (DataRegionMetrics m : ig.dataRegionMetrics())
                            if (m.getName().equals("dfltDataRegion"))
                                dirtyPages = m.getDirtyPages();

                        long cpBufPages = 0;

                        long cpWrittenPages;

                        AtomicInteger cntr = ((GridCacheDatabaseSharedManager)((ignite(0))
                            .context().cache().context().database())).getCheckpointer().currentProgress().writtenPagesCounter();

                        cpWrittenPages = cntr == null ? 0 : cntr.get();

                        try {
                            cpBufPages = ((ignite(0)).context().cache().context().database()
                                .dataRegion("dfltDataRegion").pageMemory()).checkpointBufferPagesCount();
                        }
                        catch (IgniteCheckedException e) {
                            e.printStackTrace();
                        }

                        System.out.println("@@@ putsPerSec=," + (putRate.value()) + ", getsPerSec=," + (getRate.value()) + ", dirtyPages=," + dirtyPages + ", cpWrittenPages=," + cpWrittenPages + ", cpBufPages=," + cpBufPages);

                        try {
                            Thread.sleep(1000);
                        }
                        catch (InterruptedException ignored) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }
            }, "metrics-view");

            try (IgniteDataStreamer<Object, Object> ds = ig.dataStreamer(CACHE_NAME)) {
                ds.allowOverwrite(true);

                for (int i = 0; i < keyCnt * 10; i++) {
                    ds.addData(ThreadLocalRandom.current().nextInt(keyCnt), new TestValue(ThreadLocalRandom.current().nextInt(),
                        ThreadLocalRandom.current().nextInt()));

                    putRate.increment();
                }
            }

            run.set(false);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     *
     */
    private static class TestValue implements Serializable {
        /** */
        private final int v1;

        /** */
        private final int v2;

        /** */
        private byte[] payload = new byte[400 + ThreadLocalRandom.current().nextInt(20)];

        /**
         * @param v1 Value 1.
         * @param v2 Value 2.
         */
        private TestValue(int v1, int v2) {
            this.v1 = v1;
            this.v2 = v2;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TestValue val = (TestValue)o;

            return v1 == val.v1 && v2 == val.v2;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = v1;

            res = 31 * res + v2;

            return res;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(TestValue.class, this);
        }
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    private void deleteWorkFiles() throws Exception {
        cleanPersistenceDir();
        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), "snapshot", false));
    }
}
