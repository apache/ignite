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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.Collections;
import java.util.function.Consumer;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.internal.processors.cache.GridCacheAdapter.NON_TRANSACTIONAL_IGNITE_CACHE_CLEAR_IN_TX_ERROR_MESSAGE;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/** Checks that non-transactional cache operations fail within a transaction. */
public class NonTransactionalOperationsInTxTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        return super.getConfiguration(gridName).setCacheConfiguration(defaultCacheConfiguration()
            .setAtomicityMode(TRANSACTIONAL));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** */
    @Test
    public void testIgniteCacheClear() throws Exception {
        startGrid(0);

        checkClearOperation(grid(0));
    }

    /** */
    @Test
    public void testIgniteCacheClearOnClientNode() throws Exception {
        startGrid(0);

        startClientGrid(1);

        checkClearOperation(grid(1));
    }

    /**
     * It should throw exception.
     *
     * @param ignite Ignite.
     */
    private void checkClearOperation(IgniteEx ignite) {
        checkIgniteCacheOperation(ignite, false, cache -> cache.clear());

        checkIgniteCacheOperation(ignite, false, cache -> cache.clear(2));

        checkIgniteCacheOperation(ignite, false, cache -> cache.clear(Collections.singleton(2)));

        checkIgniteCacheOperation(ignite, false, cache -> cache.clearAll(Collections.singleton(2)));

        checkIgniteCacheOperation(ignite, false, cache -> cache.clearAsync());

        checkIgniteCacheOperation(ignite, false, cache -> cache.clearAsync(2));

        checkIgniteCacheOperation(ignite, false, cache -> cache.clearAllAsync(Collections.singleton(2)));

        checkIgniteCacheOperation(ignite, true, cache -> cache.localClear(2));

        checkIgniteCacheOperation(ignite, true, cache -> cache.localClearAll(Collections.singleton(2)));

        checkIgniteCacheOperation(ignite, false, cache -> cache.localClear(2));

        checkIgniteCacheOperation(ignite, false, cache -> cache.localClearAll(Collections.singleton(2)));
    }

    /**
     * It should throw exception.
     *
     * @param ignite Ignite.
     */
    private void checkIgniteCacheOperation(IgniteEx ignite, boolean near, Consumer<IgniteCache<Object, Object>> op) {
        IgniteCache<Object, Object> cache;

        if (near)
            cache = ignite.createNearCache(DEFAULT_CACHE_NAME, new NearCacheConfiguration<>());
        else
            cache = ignite.cache(DEFAULT_CACHE_NAME);

        cache.put(1, 1);

        assertThrows(null,
            () -> doInTransaction(ignite, () -> {
                cache.put(2, 2);

                op.accept(cache);

                return null;
            }),
            CacheException.class,
            NON_TRANSACTIONAL_IGNITE_CACHE_CLEAR_IN_TX_ERROR_MESSAGE
        );

        assertTrue(cache.containsKey(1));

        assertFalse(cache.containsKey(2));
    }
}
