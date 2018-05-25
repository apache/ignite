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

package org.apache.ignite.internal.processors.cache.persistence;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteSet;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.discovery.IgniteDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD;

/**
 * Advanced PDS IgniteSet consistency tests,
 */
public class IgnitePersistentStoreIgniteSetConsistencyTest extends GridCommonAbstractTest {
    /** */
    private static final int WAL_HIST_SIZE = 30_000;

    /** */
    private static final Long CHECKPOINT_FREQ = 1_000L;

    /** */
    private static final int NODES_CNT = 4;

    /** */
    private static final String SET_NAME = "set1";

    /** */
    private boolean walHistRebalance;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration memCfg = new DataStorageConfiguration();

        memCfg.setPageSize(4096);
        memCfg.setWalMode(WALMode.LOG_ONLY);

        if (walHistRebalance) {
            memCfg.setCheckpointFrequency(CHECKPOINT_FREQ);
            memCfg.setWalHistorySize(WAL_HIST_SIZE);
            memCfg.setWalMode(WALMode.FSYNC);

            System.setProperty(IGNITE_PDS_WAL_REBALANCE_THRESHOLD, "0");
        }

        memCfg.setDefaultDataRegionConfiguration(new DataRegionConfiguration()
            .setMaxSize(100 * 1024 * 1024)
            .setPersistenceEnabled(true));

        cfg.setDataStorageConfiguration(memCfg);
        cfg.setAutoActivationEnabled(false);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        grid(0).cluster().active(false);

        stopAllGrids();

        cleanPersistenceDir();

        System.clearProperty(IGNITE_PDS_WAL_REBALANCE_THRESHOLD);
    }

    /**
     * Check IgniteSet consistency after node leaves and joins to the cluster.
     *
     * @throws Exception If failed.
     */
    public void testNodeLeaveAndJoin() throws Exception {
        doTestRebalance(false, false);
    }

    /**
     * Check IgniteSet consistency after node  with WAL history leaves and joins to the cluster.
     *
     * @throws Exception If failed.
     */
    public void testNodeLeaveAndJoinWithHistory() throws Exception {
        doTestRebalance(true, false);
    }

    /**
     * Check collocated IgniteSet consistency after node leaves and joins to the cluster.
     *
     * @throws Exception If failed.
     */
    public void testNodeLeaveAndJoinCollocated() throws Exception {
        doTestRebalance(false, true);
    }

    /**
     * Check collocated IgniteSet consistency after node  with WAL history leaves and joins to the cluster.
     *
     * @throws Exception If failed.
     */
    public void testNodeLeaveAndJoinWithHistoryCollocated() throws Exception {
        doTestRebalance(true, true);
    }

    /**
     * @throws Exception If Failed.
     */
    public void testUpdatesReordering() throws  Exception {
        doTestUpdatesReordering(false);
    }

    /**
     * @throws Exception If Failed.
     */
    public void testUpdatesReorderingCollocated() throws  Exception {
        doTestUpdatesReordering(true);
    }

    /**
     * @throws Exception If Failed.
     */
    public void testAddRemoveAsync() throws  Exception {
        doTestAddRemoveAsync(false);
    }

    /**
     * @throws Exception If Failed.
     */
    public void testAddRemoveAsyncCollocated() throws  Exception {
        doTestAddRemoveAsync(true);
    }

    /**
     * @param useWalHist Use partial rebalance (using WAL history).
     * @throws Exception if failed.
     */
    private void doTestRebalance(boolean useWalHist, boolean collocated) throws Exception {
        this.walHistRebalance = useWalHist;

        startGrids(NODES_CNT);

        Ignite node = grid(0);

        node.cluster().active(true);

        IgniteSet<Integer> set0 = node.set(SET_NAME, config(collocated, collocated ? NODES_CNT - 1 : 1));

        assertEquals(0, set0.size());

        for (int i = 0; i < 1024; i++)
            set0.add(i);

        killNode(3);

        for (int i = 1024; i < 2048; i++)
            set0.add(i);

        for (int i = 0; i < 512; i++)
            set0.remove(i);

        log.info("Return node to cluster.");

        Ignite node3 = startGrid(3);

        IgniteSet<Integer> set3 = node3.set(SET_NAME, null);

        assertEquals(set0.size(), set3.size());
        assertEquals(1536, set3.size());

        awaitPartitionMapExchange();

        assertEquals(set0.size(), set3.size());
        assertEquals(1536, set3.size());
    }


    /**
     * @throws Exception If Failed.
     * @param collocated Collocation flag.
     */
    private void doTestAddRemoveAsync(boolean collocated) throws Exception {
        final int TOTAL = 190_000;
        final int DELTA = 10_000;
        final int MAX = TOTAL + DELTA;

        List<Integer> data = new ArrayList<>(MAX);

        Ignite ignite = startGrids(NODES_CNT);

        for (int i = 0; i < MAX; i++)
            data.add(i);

        List<Integer> initData = data.subList(0, TOTAL);
        List<Integer> rmvData = data.subList(0, DELTA);
        List<Integer> addData = data.subList(TOTAL, MAX);

        ignite.cluster().active(true);

        IgniteSet<Integer> set = ignite.set(SET_NAME, config(collocated, 0));

        assertEquals(0, set.size());

        set.addAll(initData);

        assertEquals(TOTAL, set.size());

        stopAllGrids();

        ignite = startGrids(NODES_CNT);

        ignite.cluster().active(true);

        final IgniteSet<Integer> set0 = ignite.set(SET_NAME, null);

        IgniteInternalFuture fut = GridTestUtils.runAsync(() -> {
            set0.removeAll(rmvData);
        });

        set0.addAll(addData);

        fut.get();

        assertEquals(TOTAL, set0.size());
    }

    /**
     * @param collocated Collocation flag.
     * @throws Exception If failed.
     */
    private void doTestUpdatesReordering(boolean collocated) throws Exception {
        Ignite ignite = startGrids(NODES_CNT);

        ignite.cluster().active(true);

        IgniteSet<Integer> set = ignite.set(SET_NAME, config(collocated, 0));

        assertEquals(0, set.size());

        for (int i = 0; i < 200_000; i++)
            set.add(i);

        stopAllGrids();

        ignite = startGrids(NODES_CNT);

        ignite.cluster().active(true);

        set = ignite.set(SET_NAME, null);

        // Check operations reordering.
        for (int i = 0; i < 200_000; i += 20) {
            set.add(i);
            set.remove(i);
        };

        assertEquals(190_000, set.size());
    }

    /**
     * @param collocated Collocation flag.
     * @return Collection configuration.
     */
    private CollectionConfiguration config(boolean collocated, int backups) {
        CollectionConfiguration conf = new CollectionConfiguration();

        conf.setBackups(backups);
        conf.setCollocated(collocated);

        return conf;
    }

    /**
     * @param idx Node index.
     */
    private void killNode(int idx) {
        log.info("Kill node " + grid(idx).localNode().id());

        IgniteConfiguration cfg = grid(idx).configuration();

        ((IgniteDiscoverySpi)cfg.getDiscoverySpi()).simulateNodeFailure();

        stopGrid(idx);
    }
}
