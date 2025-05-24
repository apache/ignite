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

package org.apache.ignite.internal.processors.cache.persistence.freelist;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.ignite.internal.processors.cache.persistence.freelist.PagesList.IGNITE_PAGES_LIST_STRIPES_PER_BUCKET;
import static org.apache.ignite.testframework.GridTestUtils.*;

/**
 * Test freelists.
 */
@WithSystemProperty(key = IGNITE_PAGES_LIST_STRIPES_PER_BUCKET, value = "1")
public class FreeListTest extends GridCommonAbstractTest {
    /** */
    private static final int KEYS_COUNT = 5_000;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();

        int pageSize = dsCfg.getPageSize() == 0 ? DataStorageConfiguration.DFLT_PAGE_SIZE : dsCfg.getPageSize();

        dsCfg.setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(false)
                .setMaxSize(pageSize * 3L * KEYS_COUNT));

        cfg.setDataStorageConfiguration(dsCfg);

        return cfg;
    }

    /** */
    @Test
    public void testConcurrentUpdatesAndRemoves() throws Exception {
        IgniteEx ignite = startGrid(0);

        IgniteCache<Object, Object> cache = ignite.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC));

        Random random = new Random(3793);

        for (int i = 0; i < KEYS_COUNT; i++)
            cache.put(i, getRecord(random));

        AtomicBoolean stop = new AtomicBoolean(false);

        IgniteInternalFuture<Long> updateFuture = runMultiThreadedAsync(() -> {
            while (!stop.get()) {
                try {
                    cache.put(random.nextInt(KEYS_COUNT), getRecord(random));
                }
                catch (Exception ex) {
                    stop.set(true);

                    throw ex;
                }
            }
        }, 24, "update");

        for (int i = 0; i < KEYS_COUNT * 200 && !stop.get(); i++) {
            if (i % 50000 == 0)
                ignite.log().info(String.format("Remove [i=%d, cachSize=%d]", i, cache.size()));

            cache.remove(random.nextInt(KEYS_COUNT));
        }

        stop.set(true);

        updateFuture.get();
    }

    /** */
    private static byte[] getRecord(Random random) {
        return new byte[random.nextInt(3000, 15000)];
    }
}
