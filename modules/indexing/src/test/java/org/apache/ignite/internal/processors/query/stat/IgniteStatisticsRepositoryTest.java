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

package org.apache.ignite.internal.processors.query.stat;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.managers.systemview.GridSystemViewManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetastorageLifecycleListener;
import org.apache.ignite.internal.processors.metastorage.persistence.ReadWriteMetaStorageMock;
import org.apache.ignite.internal.processors.subscription.GridInternalSubscriptionProcessor;
import org.apache.ignite.internal.util.collection.IntMap;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.logger.GridTestLog4jLogger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

/**
 * Test for statistics repository.
 */
@RunWith(Parameterized.class)
public class IgniteStatisticsRepositoryTest extends StatisticsAbstractTest {
    /** First default key. */
    protected static final StatisticsKey K1 = new StatisticsKey(SCHEMA, "tab1");

    /** Second default key. */
    protected static final StatisticsKey K2 = new StatisticsKey(SCHEMA, "tab2");

    /** Column statistics with 100 nulls. */
    protected ColumnStatistics cs1 = new ColumnStatistics(null, null, 100, 0, 100,
        0, new byte[0], 0, U.currentTimeMillis());

    /** Column statistics with 100 integers 0-100. */
    protected ColumnStatistics cs2 = new ColumnStatistics(new BigDecimal(0), new BigDecimal(100), 0, 100, 100,
        4, new byte[0], 0, U.currentTimeMillis());

    /** Column statistics with 0 rows. */
    protected ColumnStatistics cs3 = new ColumnStatistics(null, null, 0, 0, 0, 0,
        new byte[0], 0, U.currentTimeMillis());

    /** Column statistics with 100 integers 0-10. */
    protected ColumnStatistics cs4 = new ColumnStatistics(new BigDecimal(0), new BigDecimal(10), 0, 10, 100,
        4, new byte[0], 0, U.currentTimeMillis());

    /** Persistence enabled flag. */
    @Parameterized.Parameter(value = 0)
    public boolean persist;

    /** Repository to test. */
    @Parameterized.Parameter(value = 1)
    public IgniteStatisticsRepository repo;

    /** Parameters: boolean, store.
     * boolean - is persistence store;
     * store - store instance.
     */
    @Parameterized.Parameters(name = "persist={0}")
    public static List<Object[]> parameters() throws IgniteCheckedException {
        ArrayList<Object[]> params = new ArrayList<>();

        // Without persistence
        IgniteStatisticsStore storeInMemory = new IgniteStatisticsInMemoryStoreImpl(
            IgniteStatisticsRepositoryTest::getLogger);
        GridSystemViewManager sysViewMgr = Mockito.mock(GridSystemViewManager.class);
        IgniteStatisticsRepository inMemRepo = new IgniteStatisticsRepository(storeInMemory, sysViewMgr, null,
            IgniteStatisticsRepositoryTest::getLogger);
        params.add(new Object[] {false, inMemRepo});

        // With persistence
        MetastorageLifecycleListener lsnr[] = new MetastorageLifecycleListener[1];

        GridInternalSubscriptionProcessor subscriptionProcessor = Mockito.mock(GridInternalSubscriptionProcessor.class);
        Mockito.doAnswer(invocation -> lsnr[0] = invocation.getArgument(0))
            .when(subscriptionProcessor).registerMetastorageListener(Mockito.any(MetastorageLifecycleListener.class));
        IgniteCacheDatabaseSharedManager db = Mockito.mock(IgniteCacheDatabaseSharedManager.class);

        IgniteStatisticsRepository statsRepos[] = new IgniteStatisticsRepository[1];
        IgniteStatisticsStore storePersistent = new IgniteStatisticsPersistenceStoreImpl(subscriptionProcessor, db,
            IgniteStatisticsRepositoryTest::getLogger);
        IgniteStatisticsHelper helper = Mockito.mock(IgniteStatisticsHelper.class);
        statsRepos[0] = new IgniteStatisticsRepository(storePersistent, sysViewMgr, helper,
            IgniteStatisticsRepositoryTest::getLogger);

        ReadWriteMetaStorageMock metastorage = new ReadWriteMetaStorageMock();
        lsnr[0].onReadyForReadWrite(metastorage);

        params.add(new Object[]{true, statsRepos[0]});

        return params;
    }

    /**
     * Logger builder.
     *
     * @param cls Class to build logger by.
     * @return Ignite logger.
     */
    private static IgniteLogger getLogger(Class cls) {
        return new GridTestLog4jLogger();
    }

    /**
     * Test specified statistics repository with partitions statistics.
     *
     * 1) Generate two object key and few partition statistics.
     * 2) Check that there are no statistics before tests.
     * 3) Put local partition statistics.
     * 4) Read and check partition statistics one by one.
     * 5) Read all partition statistics by object and check its size.
     */
    @Test
    public void testRepositoryPartitions() {
        ObjectPartitionStatisticsImpl stat1 = getPartitionStatistics(1);
        ObjectPartitionStatisticsImpl stat10 = getPartitionStatistics(10);

        ObjectPartitionStatisticsImpl stat1_2 = getPartitionStatistics(1);

        assertTrue(repo.getLocalPartitionsStatistics(K1).isEmpty());
        assertTrue(repo.getLocalPartitionsStatistics(K2).isEmpty());

        IgniteStatisticsStore store = repo.statisticsStore();
        store.saveLocalPartitionStatistics(K1, stat1);
        store.saveLocalPartitionStatistics(K1, stat10);
        store.saveLocalPartitionStatistics(K2, stat1_2);

        ObjectPartitionStatisticsImpl stat1Readed = repo.getLocalPartitionStatistics(K1, 1);
        assertNotNull(stat1Readed);
        assertEquals(1, stat1Readed.partId());

        ObjectPartitionStatisticsImpl stat10Readed = repo.getLocalPartitionStatistics(K1, 10);
        assertNotNull(stat10Readed);
        assertEquals(10, stat10Readed.partId());

        assertNull(repo.getLocalPartitionStatistics(K1, 2));

        assertEquals(2, repo.getLocalPartitionsStatistics(K1).size());
        assertEquals(1, repo.getLocalPartitionsStatistics(K2).size());
    }

    /**
     * Save few object partition statistics, delete some of them and check the results.
     */
    @Test
    public void testClearLocalPartitionIdsStatistics() {
        ObjectPartitionStatisticsImpl stat1 = getPartitionStatistics(1);
        ObjectPartitionStatisticsImpl stat10 = getPartitionStatistics(10);
        ObjectPartitionStatisticsImpl stat100 = getPartitionStatistics(100);

        IgniteStatisticsStore store = repo.statisticsStore();
        store.saveLocalPartitionStatistics(K1, stat1);
        store.saveLocalPartitionStatistics(K1, stat10);
        store.saveLocalPartitionStatistics(K1, stat100);

        assertNotNull(repo.getLocalPartitionStatistics(K1, 10));

        repo.clearLocalPartitionsStatistics(K1, setOf(1, 2, 10));

        assertNull(repo.getLocalPartitionStatistics(K1, 10));
        assertNotNull(repo.getLocalPartitionStatistics(K1, 100));
    }

    /**
     * Test local statistics processing:
     *
     * 1) Save two local statistics.
     * 2) Check it.
     * 3) Remove all partitions statistics by first key.
     * 4) Check it and endure the second one are still presented in repo.
     */
    @Test
    public void testLocalStatistics() {
        ObjectStatisticsImpl stat1 = getStatistics();
        ObjectStatisticsImpl stat2 = getStatistics();

        repo.saveLocalStatistics(K1, stat1, AffinityTopologyVersion.ZERO);
        repo.saveLocalStatistics(K2, stat2, AffinityTopologyVersion.ZERO);

        assertEquals(stat1, repo.getLocalStatistics(K1, null));
        assertEquals(stat1, repo.getLocalStatistics(K2, AffinityTopologyVersion.ZERO));

        repo.clearLocalPartitionsStatistics(K1, null);

        assertNull(repo.getLocalStatistics(K1, AffinityTopologyVersion.ZERO));
        assertEquals(stat2, repo.getLocalStatistics(K2, null));
    }

    /**
     * Test refresh for partition obsolescence info:
     *
     * 1) Try to refresh partition obsolescence info.
     * 2) Get it as key.
     * 3) Check there is data in store.
     * 4) Mark info as durty
     * 5) Save obsolescence by right key.
     * 6) Check there is obsolescence info in store.
     * 7) Check there is clean object in repo.
     */
    @Test
    public void testRefreshObsolescence() {
        IgniteStatisticsStore store = repo.statisticsStore();

        repo.stop();
        store.clearAllStatistics();
        repo.start();

        // 1) Try to refresh partition obsolescence info.
        repo.refreshObsolescence(K1, 1);

        // 2) Get it as key.
        List<StatisticsKey> keys = repo.getObsolescenceKeys();

        assertEquals(1, keys.size());

        // 3) Check there is data in store.
        Map<StatisticsKey, IntMap<ObjectPartitionStatisticsObsolescence>> allObs = store.loadAllObsolescence();

        assertNotNull(allObs.get(K1));

        // 4) Mark info as durty.
        repo.getObsolescence(K1, 1).onModified(new byte[]{1, 2, 3});

        // 5) Save obsolescence by right key.
        repo.saveObsolescenceInfo(K1);

        // 6) Check there is obsolescence info in store.
        allObs = store.loadAllObsolescence();

        assertEquals(1L, allObs.get(K1).get(1).modified());

        // 7) Check there is clean object in repo.
        assertFalse(repo.getObsolescence(K1, 1).dirty());
    }

    /**
     * Save obsolescence info and check it saved into the store.
     */
    @Test
    public void testSaveObsolescenceInfo() {
        IgniteStatisticsStore store = repo.statisticsStore();

        Map<StatisticsKey, IntMap<ObjectPartitionStatisticsObsolescence>> statObs = GridTestUtils
            .getFieldValue(repo, "statObs");

        repo.refreshObsolescence(K1, 2);
        repo.saveObsolescenceInfo(K1);

        assertNotNull(statObs.get(K1).get(2));
    }
}
