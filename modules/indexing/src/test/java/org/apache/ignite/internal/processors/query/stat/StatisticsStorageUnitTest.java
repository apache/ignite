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

import java.util.Arrays;
import java.util.Collection;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.managers.systemview.GridSystemViewManager;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetastorageLifecycleListener;
import org.apache.ignite.internal.processors.metastorage.persistence.ReadWriteMetaStorageMock;
import org.apache.ignite.internal.processors.subscription.GridInternalSubscriptionProcessor;
import org.apache.ignite.testframework.junits.logger.GridTestLog4jLogger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

/**
 * Unit tests for statistics store.
 */
@RunWith(Parameterized.class)
public class StatisticsStorageUnitTest extends StatisticsAbstractTest {
    /** Test statistics key1. */
    private static final StatisticsKey KEY1 = new StatisticsKey("schema", "obj");

    /** Test statistics key2. */
    private static final StatisticsKey KEY2 = new StatisticsKey("schema", "obj2");

    /** Test against storage of such type. */
    @Parameterized.Parameter(0)
    public String testLb;

    /** Test store. */
    @Parameterized.Parameter(1)
    public IgniteStatisticsStore store;

    /**
     * @return Test parameters.
     */
    @Parameterized.Parameters(name = "cacheMode={0}")
    public static Collection<Object[]> parameters() throws IgniteCheckedException {

        MetastorageLifecycleListener lsnr[] = new MetastorageLifecycleListener[1];

        IgniteStatisticsHelper helper = Mockito.mock(IgniteStatisticsHelper.class);

        GridInternalSubscriptionProcessor subscriptionProcessor = Mockito.mock(GridInternalSubscriptionProcessor.class);
        Mockito.doAnswer(invocation -> lsnr[0] = invocation.getArgument(0))
            .when(subscriptionProcessor).registerMetastorageListener(Mockito.any(MetastorageLifecycleListener.class));

        IgniteStatisticsStore inMemoryStore = new IgniteStatisticsInMemoryStoreImpl(cls -> log);
        GridSystemViewManager sysViewMgr = Mockito.mock(GridSystemViewManager.class);

        IgniteStatisticsRepository statsRepos = new IgniteStatisticsRepository(inMemoryStore, sysViewMgr, helper, cls -> log);

        IgniteCacheDatabaseSharedManager dbMgr = new IgniteCacheDatabaseSharedManager();
        IgniteStatisticsPersistenceStoreImpl persStore = new IgniteStatisticsPersistenceStoreImpl(subscriptionProcessor,
            dbMgr, cls -> new GridTestLog4jLogger());

        ReadWriteMetaStorageMock metastorage = new ReadWriteMetaStorageMock();
        lsnr[0].onReadyForReadWrite(metastorage);

        return Arrays.asList(new Object[][] {
            { "IgniteStatisticsInMemoryStoreImpl", inMemoryStore },
            { "IgniteStatisticsPersistenceStoreImpl", persStore},
        });
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        store = new IgniteStatisticsInMemoryStoreImpl(cls -> log);
    }

    /**
     * Test clear all method:
     *
     * 1) Clear store and put some statistics into it.
     * 2) Call clearAll.
     * 2) Check that saved statistics are deleted.
     */
    @Test
    public void testClearAll() {
        store.clearAllStatistics();
        store.saveLocalPartitionStatistics(KEY1, getPartitionStatistics(1));

        store.clearAllStatistics();

        assertTrue(store.getLocalPartitionsStatistics(KEY1).isEmpty());
        assertNull(store.getLocalPartitionStatistics(KEY1, 1));
    }

    /**
     * Test saving and acquiring of single partition statistics:
     *
     *  1) Save partition statistics in store.
     *  2) Load it by right key and part id.
     *  3) Load null with wrong key.
     *  4) Load null with wrong part id.
     */
    @Test
    public void testSingleOperations() {
        ObjectPartitionStatisticsImpl partStat = getPartitionStatistics(21);
        store.saveLocalPartitionStatistics(KEY1, partStat);

        assertEquals(partStat, store.getLocalPartitionStatistics(KEY1, 21));

        assertNull(store.getLocalPartitionStatistics(KEY1, 2));
        assertNull(store.getLocalPartitionStatistics(KEY2, 1));
    }

    /**
     * Test saving and acquiring set of partition statistics:
     *
     * 1) Save a few statistics with group replace method.
     * 2) Check that group load methods return correct number of partition statistics with right and wrong keys.
     */
    @Test
    public void testGroupOperations() {
        ObjectPartitionStatisticsImpl partStat1 = getPartitionStatistics(101);
        ObjectPartitionStatisticsImpl partStat2 = getPartitionStatistics(102);
        ObjectPartitionStatisticsImpl partStat3 = getPartitionStatistics(103);
        store.replaceLocalPartitionsStatistics(KEY1, Arrays.asList(partStat1, partStat2, partStat3));

        assertEquals(3, store.getLocalPartitionsStatistics(KEY1).size());
        assertEquals(0, store.getLocalPartitionsStatistics(KEY2).size());
    }
}
