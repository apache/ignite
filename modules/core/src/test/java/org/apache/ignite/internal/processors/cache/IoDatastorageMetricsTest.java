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

package org.apache.ignite.internal.processors.cache;

import java.util.Arrays;
import java.util.Collection;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * The test shows that the WalWritingRate metric is not calculated when walMode in all modes.
 */
@RunWith(Parameterized.class)
public class IoDatastorageMetricsTest extends GridCommonAbstractTest {

    /** WALMode. */
    @Parameterized.Parameter
    public WALMode walMode;

    /** WALMode values. */
    @Parameterized.Parameters(name = "walMode={0}")
    public static Collection<Object> parameters() {
        return Arrays.asList(
            WALMode.FSYNC,
            WALMode.BACKGROUND,
            WALMode.LOG_ONLY
        );
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setMetricsEnabled(true)
                    .setWalSegmentSize((int)(U.MB))
                    .setWalMode(walMode)
                    .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration()
                            .setPersistenceEnabled(true)
                    )
            );
    }

    /**
     * The test shows that the WalWritingRate metric is not calculated when walMode in all modes.
     */
    @Test
    public void walWritingRate() throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.cluster().state(ClusterState.ACTIVE);

        long writingRate = 0;

        byte i = 0;
        for (; i < 100; i++) {
            long[] arr = new long[64];

            Arrays.fill(arr, i);

            ignite.getOrCreateCache(DEFAULT_CACHE_NAME).put(i, arr);

            writingRate = (long)metricRegistry(ignite.name(), "io", "datastorage").getAttribute("WalWritingRate");

            if (writingRate > 0)
                break;
        }

        assertTrue(writingRate > 0);
    }
}
