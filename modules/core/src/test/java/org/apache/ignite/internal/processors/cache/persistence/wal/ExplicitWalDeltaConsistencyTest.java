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
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.wal.memtracker.PageMemoryTrackerPluginProvider;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * WAL delta records consistency test with explicit checks.
 */
@RunWith(JUnit4.class)
public class ExplicitWalDeltaConsistencyTest extends AbstractWalDeltaConsistencyTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public final void testPutRemoveAfterCheckpoint() throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.cluster().active(true);

        IgniteCache<Integer, Object> cache = ignite.createCache(cacheConfiguration(DEFAULT_CACHE_NAME));

        for (int i = 0; i < 5_000; i++)
            cache.put(i, "Cache value " + i);

        for (int i = 1_000; i < 2_000; i++)
            cache.put(i, i);

        for (int i = 500; i < 1_500; i++)
            cache.remove(i);

        assertTrue(PageMemoryTrackerPluginProvider.tracker(ignite).checkPages(true));

        forceCheckpoint();

        for (int i = 3_000; i < 10_000; i++)
            cache.put(i, "Changed cache value " + i);

        for (int i = 4_000; i < 7_000; i++)
            cache.remove(i);

        assertTrue(PageMemoryTrackerPluginProvider.tracker(ignite).checkPages(true));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public final void testNotEmptyPds() throws Exception {
        Assume.assumeFalse("https://issues.apache.org/jira/browse/IGNITE-10822", MvccFeatureChecker.forcedMvcc());

        IgniteEx ignite = startGrid(0);

        ignite.cluster().active(true);

        IgniteCache<Integer, Object> cache = ignite.createCache(cacheConfiguration(DEFAULT_CACHE_NAME));

        for (int i = 0; i < 3_000; i++)
            cache.put(i, "Cache value " + i);

        forceCheckpoint();

        stopGrid(0);

        ignite = startGrid(0);

        ignite.cluster().active(true);

        cache = ignite.getOrCreateCache(cacheConfiguration(DEFAULT_CACHE_NAME));

        for (int i = 2_000; i < 5_000; i++)
            cache.put(i, "Changed cache value " + i);

        for (int i = 1_000; i < 4_000; i++)
            cache.remove(i);

        assertTrue(PageMemoryTrackerPluginProvider.tracker(ignite).checkPages(true));
    }
}
