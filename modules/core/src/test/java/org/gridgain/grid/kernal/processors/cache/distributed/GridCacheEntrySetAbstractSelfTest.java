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

package org.gridgain.grid.kernal.processors.cache.distributed;

import org.apache.ignite.transactions.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.testframework.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.apache.ignite.transactions.IgniteTxConcurrency.*;
import static org.apache.ignite.transactions.IgniteTxIsolation.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 *
 */
public abstract class GridCacheEntrySetAbstractSelfTest extends GridCacheAbstractSelfTest {
    /** */
    private static final int GRID_CNT = 2;

    /** */
    private static final String TX_KEY = "txKey";

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return GRID_CNT;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheWriteSynchronizationMode writeSynchronization() {
        return FULL_SYNC;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /**
     * @throws Exception If failed.
     */
    public void testEntrySet() throws Exception {
        for (int i = 0; i < 10; i++) {
            log.info("Iteration: " + i);

            final AtomicInteger cacheIdx = new AtomicInteger(0);

            GridTestUtils.runMultiThreaded(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    int idx = cacheIdx.getAndIncrement();

                    log.info("Use cache " + idx);

                    GridCache<Object, Object> cache = grid(idx).cache(null);

                    for (int i = 0; i < 100; i++)
                        putAndCheckEntrySet(cache);

                    return null;
                }
            }, GRID_CNT, "test");

            for (int j = 0; j < gridCount(); j++)
                cache(j).removeAll();
        }
    }

    /**
     * @param cache Cache.
     * @throws Exception If failed.
     */
    private void putAndCheckEntrySet(GridCache<Object, Object> cache) throws Exception {
        try (IgniteTx tx = cache.txStart(PESSIMISTIC, REPEATABLE_READ)) {
            Integer total = (Integer) cache.get(TX_KEY);

            if (total == null)
                total = 0;

            int cntr = 0;

            Set<GridCacheEntry<Object, Object>> entries = cache.entrySet();

            for (GridCacheEntry e : entries) {
                if (e.getKey() instanceof Integer)
                    cntr++;
            }

            assertEquals(total, (Integer)cntr);

            cache.putx(cntr + 1, cntr + 1);

            cache.putx(TX_KEY, cntr + 1);

            tx.commit();
        }
    }
}
