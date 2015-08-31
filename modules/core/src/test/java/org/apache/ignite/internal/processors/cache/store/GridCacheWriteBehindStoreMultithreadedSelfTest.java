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
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Multithreaded tests for {@link org.apache.ignite.internal.processors.cache.store.GridCacheWriteBehindStore}.
 */
public class GridCacheWriteBehindStoreMultithreadedSelfTest extends GridCacheWriteBehindStoreAbstractSelfTest {
    /**
     * This test performs complex set of operations on store from multiple threads.
     *
     * @throws Exception If failed.
     */
    public void testPutGetRemove() throws Exception {
        initStore(2);

        Set<Integer> exp;

        try {
            exp = runPutGetRemoveMultithreaded(10, 10);
        }
        finally {
            shutdownStore();
        }

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

    /**
     * Tests that cache would keep values if underlying store fails.
     *
     * @throws Exception If failed.
     */
    public void testStoreFailure() throws Exception {
        delegate.setShouldFail(true);

        initStore(2);

        Set<Integer> exp;

        try {
            exp = runPutGetRemoveMultithreaded(10, 10);

            U.sleep(FLUSH_FREQUENCY);

            info(">>> There are " + store.getWriteBehindErrorRetryCount() + " entries in RETRY state");

            delegate.setShouldFail(false);

            // Despite that we set shouldFail flag to false, flush thread may just have caught an exception.
            // If we move store to the stopping state right away, this value will be lost. That's why this sleep
            // is inserted here to let all exception handlers in write-behind store exit.
            U.sleep(1000);
        }
        finally {
            shutdownStore();
        }

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

    /**
     * Tests store consistency in case of high put rate, when flush is performed from the same thread
     * as put or remove operation.
     *
     * @throws Exception If failed.
     */
    public void testFlushFromTheSameThread() throws Exception {
        // 50 milliseconds should be enough.
        delegate.setOperationDelay(50);

        initStore(2);

        Set<Integer> exp;

        int start = store.getWriteBehindTotalCriticalOverflowCount();

        try {
            //We will have in total 5 * CACHE_SIZE keys that should be enough to grow map size to critical value.
            exp = runPutGetRemoveMultithreaded(5, CACHE_SIZE);
        }
        finally {
            log.info(">>> Done inserting, shutting down the store");

            shutdownStore();
        }

        // Restore delay.
        delegate.setOperationDelay(0);

        Map<Integer, String> map = delegate.getMap();

        int end = store.getWriteBehindTotalCriticalOverflowCount();

        log.info(">>> There are " + exp.size() + " keys in store, " + (end - start) + " overflows detected");

        assertTrue("No cache overflows detected (a bug or too few keys or too few delay?)", end > start);

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