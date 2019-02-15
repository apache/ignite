/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cache.persistence;

import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 *
 */
@RunWith(JUnit4.class)
public class IgnitePdsPageSizesTest extends GridCommonAbstractTest {
    /** Cache name. */
    private final String cacheName = "cache";

    /** */
    private int pageSize;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setMaxSize(100L * 1024 * 1024).setPersistenceEnabled(true))
            .setWalMode(WALMode.LOG_ONLY)
            .setPageSize(pageSize);

        cfg.setDataStorageConfiguration(memCfg);

        cfg.setCacheConfiguration(
            new CacheConfiguration(cacheName)
                .setAffinity(new RendezvousAffinityFunction(false, 32))
        );

        cfg.setFailureDetectionTimeout(20_000);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        cleanPersistenceDir();
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testPageSize_1k() throws Exception {
        checkPageSize(1024);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testPageSize_2k() throws Exception {
        checkPageSize(2 * 1024);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testPageSize_4k() throws Exception {
        checkPageSize(4 * 1024);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testPageSize_8k() throws Exception {
        checkPageSize(8 * 1024);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testPageSize_16k() throws Exception {
        checkPageSize(16 * 1024);
    }

    /**
     * @throws Exception if failed.
     */
    private void checkPageSize(int pageSize) throws Exception {
        this.pageSize = pageSize;

        IgniteEx ignite = startGrid(0);

        ignite.active(true);

        try {
            final IgniteCache<Object, Object> cache = ignite.cache(cacheName);
            final long endTime = System.currentTimeMillis() + 60_000;

            GridTestUtils.runMultiThreaded(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    Random rnd = ThreadLocalRandom.current();

                    while (System.currentTimeMillis() < endTime) {
                        for (int i = 0; i < 500; i++)
                            cache.put(rnd.nextInt(100_000), rnd.nextInt());
                    }

                    return null;
                }
            }, 16, "runner");
        }
        finally {
            stopAllGrids();
        }
    }
}
