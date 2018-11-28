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

package org.apache.ignite.internal.processors.cache.store;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Multithreaded tests for {@link org.apache.ignite.internal.processors.cache.store.GridCacheWriteBehindStore}.
 */
public class GridCacheWriteBehindStoreMultithreadedSelfTest extends GridCacheWriteBehindStoreAbstractSelfTest {
    /**
     * Tests store (with write coalescing) consistency in case of high put rate,
     * when flush is performed from the same thread as put or remove operation.
     *
     * @throws Exception If failed.
     */
    public void testFlushFromTheSameThreadWithCoalescing() throws Exception {
        testFlushFromTheSameThread(true);
    }

    /**
     * Tests store consistency in case of high put rate, when flush is performed from the same thread
     * as put or remove operation.
     *
     * @param writeCoalescing write coalescing flag.
     * @throws Exception If failed.
     */
    private void testFlushFromTheSameThread(boolean writeCoalescing) throws Exception {
        // 50 milliseconds should be enough.
        delegate.setOperationDelay(50);

        Set<Integer> exp = null;

        int start = 0;
        int end = 0;

        long startTime = System.currentTimeMillis();

        while (end - start == 0 && System.currentTimeMillis() - startTime < getTestTimeout()) {
            initStore(2, writeCoalescing);

            start = store.getWriteBehindTotalCriticalOverflowCount();

            try {
                //We will have in total 5 * CACHE_SIZE keys that should be enough to grow map size to critical value.
                exp = runPutGetRemoveMultithreaded(5, CACHE_SIZE);
            }
            finally {
                log.info(">>> Done inserting, shutting down the store");

                shutdownStore();
            }

            end = store.getWriteBehindTotalCriticalOverflowCount();
        }

        // Restore delay.
        delegate.setOperationDelay(0);

        assertNotNull(exp);

        log.info(">>> There are " + exp.size() + " keys in store, " + (end - start) + " overflows detected");

        assertTrue("No cache overflows detected (a bug or too few keys or too few delay?)", end > start);

        Map<Integer, String> map = delegate.getMap();

        Collection<Integer> extra = new HashSet<>(map.keySet());

        extra.removeAll(exp);

        assertTrue("The underlying store contains extra keys: " + extra, extra.isEmpty());

        Collection<Integer> missing = new HashSet<>(exp);

        missing.removeAll(map.keySet());

        assertTrue("Missing keys in the underlying store: " + missing, missing.isEmpty());

        for (Integer key : exp)
            assertEquals("Invalid value for key " + key, "val" + key, map.get(key));
    }
}
