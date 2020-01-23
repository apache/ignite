/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.GridTestUtils.SF;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.TransactionRollbackException;
import org.junit.Assume;
import org.junit.Test;

/**
 * Cause by https://issues.apache.org/jira/browse/IGNITE-7278
 */
public class IgnitePdsContinuousRestartTest extends GridCommonAbstractTest {
    /** */
    private static final int GRID_CNT = 4;

    /** */
    private static final int ENTRIES_COUNT = 10_000;

    /** */
    protected static final String CACHE_NAME = "cache1";

    /** Checkpoint delay. */
    private volatile int checkpointDelay = -1;

    /** */
    private boolean cancel = false;

    /**
     * Default constructor.
     */
    public IgnitePdsContinuousRestartTest() {
    }

    /**
     * @param cancel Cancel.
     */
    protected IgnitePdsContinuousRestartTest(boolean cancel) {
        this.cancel = cancel;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        Assume.assumeFalse("https://issues.apache.org/jira/browse/IGNITE-11937", MvccFeatureChecker.forcedMvcc());

        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setConsistentId(gridName);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setMaxSize(400L * 1024 * 1024)
                    .setPersistenceEnabled(true))
            .setWalMode(WALMode.LOG_ONLY)
            .setCheckpointFrequency(checkpointDelay);

        cfg.setDataStorageConfiguration(memCfg);

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(CACHE_NAME);
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 128));
        ccfg.setBackups(2);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testRebalancingDuringLoad_1000_500_1_1() throws Exception {
        checkRebalancingDuringLoad(SF.apply(1000), SF.apply(500), 1, 1);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testRebalancingDuringLoad_8000_500_1_1() throws Exception {
        checkRebalancingDuringLoad(SF.apply(8000), SF.apply(500), 1, 1);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testRebalancingDuringLoad_1000_20000_1_1() throws Exception {
        checkRebalancingDuringLoad(SF.apply(1000), SF.apply(20000), 1, 1);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testRebalancingDuringLoad_8000_8000_1_1() throws Exception {
        checkRebalancingDuringLoad(SF.apply(8000), SF.apply(8000), 1, 1);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testRebalancingDuringLoad_1000_500_8_1() throws Exception {
        checkRebalancingDuringLoad(SF.apply(1000), SF.apply(500), 8, 1);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testRebalancingDuringLoad_8000_500_8_1() throws Exception {
        checkRebalancingDuringLoad(SF.apply(8000), SF.apply(500), 8, 1);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testRebalancingDuringLoad_1000_20000_8_1() throws Exception {
        checkRebalancingDuringLoad(SF.apply(1000), SF.apply(20000), 8, 1);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testRebalancingDuringLoad_8000_8000_8_1() throws Exception {
        checkRebalancingDuringLoad(SF.apply(8000), SF.apply(8000), 8, 1);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testRebalancingDuringLoad_1000_500_8_16() throws Exception {
        checkRebalancingDuringLoad(SF.apply(1000), SF.apply(500), 8, 16);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testRebalancingDuringLoad_8000_500_8_16() throws Exception {
        checkRebalancingDuringLoad(SF.apply(8000), SF.apply(500), 8, 16);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testRebalancingDuringLoad_1000_20000_8_16() throws Exception {
        checkRebalancingDuringLoad(SF.apply(1000), SF.apply(20000), 8, 16);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testRebalancingDuringLoad_8000_8000_8_16() throws Exception {
        checkRebalancingDuringLoad(SF.apply(8000), SF.apply(8000), 8, 16);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testRebalancingDuringLoad_10_10_1_1() throws Exception {
        checkRebalancingDuringLoad(10, 10, 1, 1);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testRebalancingDuringLoad_10_500_8_16() throws Exception {
        checkRebalancingDuringLoad(10, 500, 8, 16);
    }

    /**
     * @throws Exception if failed.
     */
    private void checkRebalancingDuringLoad(
        int restartDelay,
        int checkpointDelay,
        int threads,
        final int batch
    ) throws Exception {
        this.checkpointDelay = checkpointDelay;

        startGrids(GRID_CNT);

        final Ignite load = ignite(0);

        load.cluster().active(true);

        try (IgniteDataStreamer<Object, Object> s = load.dataStreamer(CACHE_NAME)) {
            s.allowOverwrite(true);

            for (int i = 0; i < ENTRIES_COUNT; i++)
                s.addData(i, i);
        }

        final AtomicBoolean done = new AtomicBoolean(false);

        IgniteInternalFuture<?> busyFut = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
            /** {@inheritDoc} */
            @Override public Object call() throws Exception {
                IgniteCache<Object, Object> cache = load.cache(CACHE_NAME);
                Random rnd = ThreadLocalRandom.current();

                while (!done.get()) {
                    Map<Integer, Person> map = new TreeMap<>();

                    for (int i = 0; i < batch; i++) {
                        int key = rnd.nextInt(ENTRIES_COUNT);

                        map.put(key, new Person("fn" + key, "ln" + key));
                    }

                    while (true) {
                        try {
                            cache.putAll(map);

                            break;
                        }
                        catch (Exception e) {
                            if (X.hasCause(e,
                                TransactionRollbackException.class,
                                ClusterTopologyException.class,
                                NodeStoppingException.class))
                                continue; // Expected types.

                            MvccFeatureChecker.assertMvccWriteConflict(e);
                        }
                    }
                }

                return null;
            }
        }, threads, "updater");

        long end = System.currentTimeMillis() + SF.apply(90000);

        Random rnd = ThreadLocalRandom.current();

        while (System.currentTimeMillis() < end) {
            int idx = rnd.nextInt(GRID_CNT - 1) + 1;

            stopGrid(idx, cancel);

            U.sleep(restartDelay);

            startGrid(idx);

            U.sleep(restartDelay);
        }

        done.set(true);

        busyFut.get();
    }

    /**
     *
     */
    static class Person implements Serializable {
        /** */
        @GridToStringInclude
        @QuerySqlField(index = true, groups = "full_name")
        private String fName;

        /** */
        @GridToStringInclude
        @QuerySqlField(index = true, groups = "full_name")
        private String lName;

        /**
         * @param fName First name.
         * @param lName Last name.
         */
        public Person(String fName, String lName) {
            this.fName = fName;
            this.lName = lName;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Person.class, this);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            IgnitePersistentStoreCacheGroupsTest.Person person = (IgnitePersistentStoreCacheGroupsTest.Person)o;

            return Objects.equals(fName, person.fName) && Objects.equals(lName, person.lName);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(fName, lName);
        }
    }
}
