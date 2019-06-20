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

package org.apache.ignite.internal.processors.cache.persistence.wal;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.wal.memtracker.PageMemoryTrackerPluginProvider;
import org.junit.Test;

/**
 * WAL delta records consistency test enabled by system property.
 */
public class SysPropWalDeltaConsistencyTest extends AbstractWalDeltaConsistencyTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        System.setProperty(PageMemoryTrackerPluginProvider.IGNITE_ENABLE_PAGE_MEMORY_TRACKER, "true");
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        System.clearProperty(PageMemoryTrackerPluginProvider.IGNITE_ENABLE_PAGE_MEMORY_TRACKER);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setPluginConfigurations();

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public final void testPutRemoveMultinode() throws Exception {
        IgniteEx ignite0 = startGrid(0);

        ignite0.cluster().active(true);

        IgniteCache<Integer, Object> cache0 = ignite0.createCache(cacheConfiguration("cache0"));

        for (int i = 0; i < 3_000; i++)
            cache0.put(i, "Cache value " + i);

        IgniteEx ignite1 = startGrid(1);

        for (int i = 2_000; i < 5_000; i++)
            cache0.put(i, "Changed cache value " + i);

        for (int i = 1_000; i < 4_000; i++)
            cache0.remove(i);

        IgniteCache<Integer, Object> cache1 = ignite1.createCache(cacheConfiguration("cache1"));

        for (int i = 0; i < 1_000; i++)
            cache1.put(i, "Cache value " + i);

        forceCheckpoint();
    }
}
