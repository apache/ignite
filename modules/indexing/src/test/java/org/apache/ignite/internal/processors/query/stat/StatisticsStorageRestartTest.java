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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.metastorage.persistence.ReadWriteMetaStorageMock;
import org.apache.ignite.internal.processors.subscription.GridInternalSubscriptionProcessor;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test statistics storage itself for restart cases.
 */
public class StatisticsStorageRestartTest extends StatisticsAbstractTest {
    /** Default subscription processor mock. */
    private GridInternalSubscriptionProcessor subscriptionProcessor;

    /** Default metastorage mock. */
    private ReadWriteMetaStorageMock metastorage;

    /** Default statistics store. */
    private IgniteStatisticsPersistenceStoreImpl statStore;

    /** Test statistics key1. */
    private StatisticsKey k1 = new StatisticsKey("A", "B");

    /** Test object partition statistics 1_1. */
    private ObjectPartitionStatisticsImpl stat1_1 = getPartitionStatistics(1);

    /** Test statistics key2. */
    private StatisticsKey k2 = new StatisticsKey("A", "B2");

    /** Test object partition statistics 2_2. */
    private ObjectPartitionStatisticsImpl stat2_2 = getPartitionStatistics(2);

    /** Test object partition statistics 2_3. */
    private ObjectPartitionStatisticsImpl stat2_3 = getPartitionStatistics(3);

    /** {@inheritDoc} */
    @Override public void beforeTest() {
        subscriptionProcessor = Mockito.mock(GridInternalSubscriptionProcessor.class);
        metastorage = new ReadWriteMetaStorageMock();
        statStore = new IgniteStatisticsPersistenceStoreImpl(
            subscriptionProcessor,
            new IgniteCacheDatabaseSharedManager(){},
            cls -> log);
    }

    /**
     * Save statistics to metastore throw one statistics metastore and then attach new statistics store to the same
     * metastorage:
     *
     * 1) create metastorage mock and pass it to statistics storage.
     * 2) save partition level statistics.
     * 3) create new statistics storage on the top of the same metastorage mock
     * 4) check that statistics storage will call statistics repository to cache partition level statistics
     * 5) check that statistics storage will read the same partition statistics
     *
     * @throws IgniteCheckedException In case of errors.
     */
    @Test
    public void testRestart() throws IgniteCheckedException {
        statStore.onReadyForReadWrite(metastorage);

        statStore.saveLocalPartitionStatistics(k1, stat1_1);
        statStore.replaceLocalPartitionsStatistics(k2, Arrays.asList(stat2_2, stat2_3));

        assertEquals(1, statStore.getLocalPartitionsStatistics(k1).size());
        assertEquals(stat1_1, statStore.getLocalPartitionStatistics(k1, 1));
        assertEquals(2, statStore.getLocalPartitionsStatistics(k2).size());
        assertEquals(stat2_2, statStore.getLocalPartitionStatistics(k2, 2));
        assertEquals(stat2_3, statStore.getLocalPartitionStatistics(k2, 3));

        IgniteStatisticsPersistenceStoreImpl statStore2 = new IgniteStatisticsPersistenceStoreImpl(subscriptionProcessor,
            new IgniteCacheDatabaseSharedManager(){}, cls -> log);

        statStore2.onReadyForReadWrite(metastorage);

        assertEquals(1, statStore2.getLocalPartitionsStatistics(k1).size());
        assertEquals(stat1_1, statStore2.getLocalPartitionStatistics(k1, 1));
        assertEquals(2, statStore2.getLocalPartitionsStatistics(k2).size());
        assertEquals(stat2_2, statStore2.getLocalPartitionStatistics(k2, 2));
        assertEquals(stat2_3, statStore2.getLocalPartitionStatistics(k2, 3));
    }

    /**
     * Test that after any deserialization error during startup reads all statistics for object will be deleted:
     *
     * 1) put into metastore some statistics for two objects.
     * 2) put into metastore some broken (A) value into statistics obj1 prefix
     * 3) put into metastore some object (B) outside the statistics prefix.
     * 4) start statistics store with such metastore
     * 5) check that log will contain error message and that object A will be deleted from metastore (with all objects
     * partition statistics), while
     * object C won't be deleted.
     *
     * @throws IgniteCheckedException In case of errors.
     */
    @Test
    public void testUnreadableStatistics() throws IgniteCheckedException {
        statStore.onReadyForReadWrite(metastorage);
        statStore.saveLocalPartitionStatistics(k1, stat1_1);
        statStore.replaceLocalPartitionsStatistics(k2, Arrays.asList(stat2_2, stat2_3));

        String statKeyInvalid = String.format("stats.data.%s.%s.%d", k1.schema(), k1.obj(), 1000);
        metastorage.write(statKeyInvalid, new byte[2]);

        String outerStatKey = "some.key.1";
        byte[] outerStatValue = new byte[] {1, 2};
        metastorage.write(outerStatKey, outerStatValue);

        IgniteStatisticsPersistenceStoreImpl statStore2 = new IgniteStatisticsPersistenceStoreImpl(subscriptionProcessor,
            new IgniteCacheDatabaseSharedManager(){}, cls -> log);

        statStore2.onReadyForReadWrite(metastorage);

        assertNull(metastorage.read(statKeyInvalid));
        assertTrue(statStore.getLocalPartitionsStatistics(k1).isEmpty());
        assertFalse(statStore.getLocalPartitionsStatistics(k2).isEmpty());
        assertTrue(Arrays.equals(outerStatValue, (byte[])metastorage.read(outerStatKey)));
    }
}
