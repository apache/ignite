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

package org.apache.ignite.internal.processors.cache.eviction.paged;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.DataPageEvictionMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.junit.Test;

/**
 * Regression test for C2: the batch-level {@code ensureFreeSpace} must evict at the configured
 * {@code evictionThreshold} (default 0.9), not silently skip eviction when the headroom exceeds the empty-pages pool.
 */
public class PageEvictionEnsureFreeSpaceThresholdTest extends PageEvictionAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        return setEvictionMode(DataPageEvictionMode.RANDOM_2_LRU, super.getConfiguration(gridName));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** */
    @Test
    public void testSequentialLargePutsEvictAtThreshold() throws Exception {
        IgniteEx ignite = startGrid(1);

        IgniteCache<Integer, Object> cache = createCache(ignite, DEFAULT_CACHE_NAME);

        long regionMax = regionMaxSize(ignite);

        int largeValSize = (int)(regionMax / 20);

        int largeRows = 25;

        for (int k = 0; k < largeRows; k++) {
            byte[] val = new byte[largeValSize];

            for (int i = 0; i < val.length; i++)
                val[i] = (byte)(k * 31 + i);

            cache.put(k, val);
        }

        assertTrue("Eviction must have started", isEvictionsStarted(ignite));

        int survived = 0;

        for (int k = 0; k < largeRows; k++) {
            byte[] read = (byte[])cache.get(k);

            if (read == null)
                continue;

            survived++;

            assertEquals(largeValSize, read.length);

            for (int i = 0; i < read.length; i++)
                assertEquals((byte)(k * 31 + i), read[i]);
        }

        assertTrue("At least one large entry must survive", survived > 0);
    }
}
