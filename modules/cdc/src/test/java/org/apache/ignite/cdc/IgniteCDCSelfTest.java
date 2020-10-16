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

import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/** */
public class IgniteCDCSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        int segmentSz = 512 * 1024;

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setWalMode(WALMode.FSYNC)
            .setMaxWalArchiveSize(10 * segmentSz)
            .setWalHistorySize(10)
            .setWalSegments(3)
            .setWalSegmentSize(segmentSz)
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true)));

        return cfg;
    }

    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        super.beforeTest();
    }

    /** Simplest CDC test. */
    @Test
    public void testCDC() throws Exception {
        IgniteCDC cdc = new IgniteCDC(getConfiguration("cdc"), new LogAllCDCConsumer());

        runAsync(cdc);

        Thread.sleep(1000);

        Ignite ign = startGrid();

        ign.cluster().state(ACTIVE);

        Callable<Object> genData = () -> {
            IgniteCache<Integer, byte[]> cache = ign.createCache("my-cache");

            while (true) {
                for (int i = 0; i < 512; i++) {
                    byte[] bytes = new byte[1024];
                    ThreadLocalRandom.current().nextBytes(bytes);

                    cache.put(i, bytes);
                }

                Thread.sleep(25000);
            }
        };

        runAsync(genData);

        Thread.sleep(60_000);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        cleanPersistenceDir();
    }
}
