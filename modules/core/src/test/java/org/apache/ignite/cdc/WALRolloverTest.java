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

package org.apache.ignite.cdc;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

public class WALRolloverTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        int _1KB = 1024 * 1024;

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setCdcEnabled(true)
            .setWalMode(WALMode.FSYNC)
            .setMaxWalArchiveSize(10L * _1KB * _1KB)
            .setWalSegmentSize(11 * _1KB)
            .setWalAutoArchiveAfterInactivity(1500L)
            .setCheckpointFrequency(150_000L)
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true)));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testWallRollover() throws Exception {
        for (int i = 0; i < 200; i++) {
            IgniteEx ign = startGrid(0);
            ign.cluster().state(ClusterState.ACTIVE);

            IgniteCache<Integer, Integer> cache = ign.getOrCreateCache("my-cache");

            for (int j = 0; j < 3; j++)
                cache.put(j, j);

            AtomicLong idx = new AtomicLong(ign.context().cache().context().wal().currentSegment());

            assertTrue(waitForCondition(() -> idx.get() < ign.context().cache().context().wal().currentSegment(),
                    getTestTimeout()));

            System.out.println("WALRolloverTest.testWallRollover =======================");

            idx.set(ign.context().cache().context().wal().currentSegment());

            ign.close();
        }
    }
}
