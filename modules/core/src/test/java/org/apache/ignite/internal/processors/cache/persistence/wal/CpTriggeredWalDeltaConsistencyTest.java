/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Checkpoint triggered WAL delta records consistency test.
 */
@RunWith(JUnit4.class)
public class CpTriggeredWalDeltaConsistencyTest extends AbstractWalDeltaConsistencyTest {
    /** {@inheritDoc} */
    @Override protected boolean checkPagesOnCheckpoint() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public final void testPutRemoveCacheDestroy() throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.cluster().active(true);

        IgniteCache<Integer, Object> cache0 = ignite.createCache(cacheConfiguration("cache0"));

        for (int i = 0; i < 3_000; i++)
            cache0.put(i, "Cache value " + i);

        for (int i = 2_000; i < 5_000; i++)
            cache0.put(i, "Changed cache value " + i);

        for (int i = 1_000; i < 4_000; i++)
            cache0.remove(i);

        for (int i = 5; i >= 0; i--) {
            IgniteCache<Integer, Object> cache1 = ignite.getOrCreateCache(cacheConfiguration("cache1"));

            for (int j = 0; j < 300; j++)
                cache1.put(j + i * 100, "Cache value " + j);

            if (i != 0)
                ignite.destroyCache("cache1");
        }

        forceCheckpoint();
    }
}
