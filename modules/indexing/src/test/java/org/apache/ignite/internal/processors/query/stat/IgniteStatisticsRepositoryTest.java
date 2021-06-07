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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.managers.systemview.GridSystemViewManager;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetastorageLifecycleListener;
import org.apache.ignite.internal.processors.metastorage.persistence.ReadWriteMetaStorageMock;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsObjectConfiguration;
import org.apache.ignite.internal.processors.subscription.GridInternalSubscriptionProcessor;
import org.apache.ignite.internal.util.collection.IntMap;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.logger.GridTestLog4jLogger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import static org.apache.ignite.internal.processors.query.stat.IgniteStatisticsHelper.buildDefaultConfigurations;

/**
 * Test for statistics repository.
 */
@RunWith(Parameterized.class)
public class IgniteStatisticsRepositoryTest extends IgniteStatisticsRepositoryStaticTest {
    /** */
    public static final StatisticsKey T1_KEY = new StatisticsKey(SCHEMA, "t1");

    /** */
    public static final StatisticsKey T2_KEY = new StatisticsKey(SCHEMA, "t2");

    /** */
    public static final StatisticsTarget T1_TARGET = new StatisticsTarget(SCHEMA, "t1", "c1", "c2", "c3");

    /** */
    public static final StatisticsTarget T2_TARGET = new StatisticsTarget(T2_KEY, "t22");

    @Parameterized.Parameter(value = 0)
    public boolean persist;

    @Parameterized.Parameter(value = 1)
    public IgniteStatisticsRepository repo;

    /** */
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

        repo.saveLocalPartitionStatistics(K1, stat1);
        repo.saveLocalPartitionStatistics(K1, stat10);
        repo.saveLocalPartitionStatistics(K2, stat1_2);

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
     * Test obsolescence work with statistics repository:
     * 1) Test no obsolescence info present.
     * 2) Check to load obsolescence by
     */
    @Test
    public void testObsolescenceLoadSave() {
        StatisticsObjectConfiguration defCfgs[] = buildDefaultConfigurations(T1_TARGET, T2_TARGET);

        Map<StatisticsObjectConfiguration, Set<Integer>> cfg = new HashMap<>();
        cfg.put(defCfgs[0], setOf(1, 3, 5, 7));
        cfg.put(defCfgs[1], setOf(2, 4, 5, 7));

        Map<StatisticsKey, IntMap<ObjectPartitionStatisticsObsolescence>> statObs = GridTestUtils
            .getFieldValue(repo, "statObs");

        IgniteStatisticsStore store = repo.statisticsStore();

        repo.stop();
        store.clearAllStatistics();
        repo.start();

        assertTrue(statObs.isEmpty());
        assertTrue(store.loadAllObsolescence().isEmpty());

        repo.checkObsolescenceInfo(cfg);

        assertFalse(statObs.isEmpty());
        assertTrue(store.loadAllObsolescence().isEmpty());

        Map<StatisticsKey, IntMap<ObjectPartitionStatisticsObsolescence>> dirty = repo.saveObsolescenceInfo();

        assertTrue(dirty.isEmpty());

        repo.addRowsModified(T2_KEY, 2, new byte[]{1, 1, 0, 100});

        assertTrue(store.loadAllObsolescence().isEmpty());

        dirty = repo.saveObsolescenceInfo();

        assertEquals(1, dirty.size());
        assertNotNull(dirty.get(T2_KEY).get(2));

        Map<StatisticsKey, IntMap<ObjectPartitionStatisticsObsolescence>> allObs = store.loadAllObsolescence();
        assertFalse(allObs.isEmpty());

        dirty = repo.saveObsolescenceInfo();

        assertTrue(dirty.isEmpty());
    }

    /**
     * Try to remove lack object.
     */
    @Test
    public void testRemoveWrongObsolescence() {
        IgniteStatisticsStore store = repo.statisticsStore();

        repo.stop();
        store.clearAllStatistics();

        repo.start();

        repo.removeObsolescenceInfo(K1);

    }

    /**
     * Test refresh for partition obsolescence info:
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

        Map<StatisticsKey, IntMap<ObjectPartitionStatisticsObsolescence>> dirty = repo.saveObsolescenceInfo();
        Map<StatisticsKey, IntMap<ObjectPartitionStatisticsObsolescence>> allObs = store.loadAllObsolescence();

        assertEquals(1, dirty.size());
        assertEquals(1, allObs.size());
    }
}
