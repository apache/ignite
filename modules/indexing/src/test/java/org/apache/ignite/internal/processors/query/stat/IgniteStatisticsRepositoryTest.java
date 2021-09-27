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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.managers.systemview.GridSystemViewManager;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetastorageLifecycleListener;
import org.apache.ignite.internal.processors.metastorage.persistence.ReadWriteMetaStorageMock;
import org.apache.ignite.internal.processors.subscription.GridInternalSubscriptionProcessor;
import org.apache.ignite.internal.util.collection.IntMap;
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
public class IgniteStatisticsRepositoryTest extends IgniteStatisticsRepositoryStaticTest {
    /** */
    public static final StatisticsKey T2_KEY = new StatisticsKey(SCHEMA, "t2");

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
     * 6) Save few partition statistics at once.
     * 7) Real all partition statistics by object and check its size.
     */
    @Test
    public void testRepositoryPartitions() {
        ObjectPartitionStatisticsImpl stat1 = getPartitionStatistics(1);
        ObjectPartitionStatisticsImpl stat10 = getPartitionStatistics(10);
        ObjectPartitionStatisticsImpl stat100 = getPartitionStatistics(100);

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

        repo.saveLocalPartitionsStatistics(K1, Arrays.asList(stat10, stat100));

        assertEquals(2, repo.getLocalPartitionsStatistics(K1).size());
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
     * Test refresh for partition obsolescence info:
     *
     * 1) Try to refresh partition obsolescence info.
     * 2) Get it as dirty object.
     */
    @Test
    public void testRefreshObsolescence() {
        IgniteStatisticsStore store = repo.statisticsStore();

        repo.stop();
        store.clearAllStatistics();
        repo.start();

        repo.refreshObsolescence(K1, 1);

        Map<StatisticsKey, IntMap<ObjectPartitionStatisticsObsolescence>> dirty = repo.getDirtyObsolescenceInfo();
        Map<StatisticsKey, IntMap<ObjectPartitionStatisticsObsolescence>> allObs = store.loadAllObsolescence();

        assertTrue(dirty.isEmpty());
        assertEquals(1, allObs.size());
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
