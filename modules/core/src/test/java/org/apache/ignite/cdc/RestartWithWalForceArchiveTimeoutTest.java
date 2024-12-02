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

import java.util.Collection;
import java.util.EnumSet;
import java.util.function.Supplier;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cdc.CdcSelfTest.addData;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;

/** */
@RunWith(Parameterized.class)
public class RestartWithWalForceArchiveTimeoutTest extends GridCommonAbstractTest {
    /** */
    @Parameterized.Parameter
    public WALMode walMode;

    /** */
    private long walForceArchiveTimeout;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setWalMode(walMode)
            .setWalForceArchiveTimeout(walForceArchiveTimeout)
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true)));

        return cfg;
    }

    /** */
    @Parameterized.Parameters(name = "walMode={0}")
    public static Collection<?> parameters() {
        return EnumSet.of(WALMode.FSYNC, WALMode.LOG_ONLY, WALMode.BACKGROUND);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids(true);

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testRestart() throws Exception {
        walForceArchiveTimeout = 60 * 60 * 1000; // 1 hour to make sure auto archive will not work.

        Supplier<IgniteEx> restart = () -> {
            stopAllGrids(true);

            try {
                IgniteEx ign = startGrid(getConfiguration("ignite-0"));

                ign.cluster().state(ACTIVE);

                return ign;
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        };

        IgniteEx ign = restart.get();

        IgniteCache<Integer, AbstractCdcTest.User> cache = ign.getOrCreateCache(DEFAULT_CACHE_NAME);

        addData(cache, 0, 100);

        for (int i = 0; i < 5; i++)
            restart.get();
    }

    /** */
    @Test
    public void testRestartAfterArchive() throws Exception {
        walForceArchiveTimeout = 1000;

        IgniteEx srv = startGrid(0);

        srv.cluster().state(ACTIVE);

        IgniteCache<Integer, Integer> cache = srv.getOrCreateCache(DEFAULT_CACHE_NAME);

        cache.put(1, 1);

        forceCheckpoint();

        Thread.sleep(2 * walForceArchiveTimeout);

        stopGrid(0);
        srv = startGrid(0);
        cache = srv.cache(DEFAULT_CACHE_NAME);

        cache.put(2, 2);

        stopGrid(0);
        srv = startGrid(0);
        cache = srv.cache(DEFAULT_CACHE_NAME);

        assertEquals(2, cache.size());
    }
}
