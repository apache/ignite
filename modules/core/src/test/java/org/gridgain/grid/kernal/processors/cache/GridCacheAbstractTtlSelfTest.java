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

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.transactions.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.store.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.testframework.*;

/**
 * Entry time-to-live abstract test.
 */
public abstract class GridCacheAbstractTtlSelfTest extends GridCacheAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheStore<?, ?> cacheStore() {
        return null;
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetExpired() throws Exception {
        final GridCache<String, Integer> c = cache();

        final String key = "1";

        int ttl = 500;

        GridCacheEntry<String, Integer> entry = c.entry(key);

        entry.timeToLive(ttl);

        entry.setValue(1);

        checkKeyIsRetired(key, ttl);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetExpiredTx() throws Exception {
        GridCache<String, Integer> c = cache();

        String key = "1";
        int ttl = 500;

        try (IgniteTx tx = c.txStart()) {
            GridCacheEntry<String, Integer> entry = c.entry(key);

            entry.timeToLive(ttl);

            entry.setValue(1);

            tx.commit();
        }

        checkKeyIsRetired(key, ttl);
    }

    /**
     * Checks if the given cache has entry with the given key with some timeout based on the given TTL.
     *
     * @param key Key to be checked.
     * @param ttl Base value for timeout before checking starts.
     * @throws Exception If failed
     */
    private void checkKeyIsRetired(final String key, int ttl) throws Exception {
        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicateX() {
            @Override public boolean applyx() throws IgniteCheckedException {
                for (int i = 0; i < gridCount(); i++) {
                    if (cache(i).get(key) != null) {
                        info("Key is still in cache of grid " + i);

                        return false;
                    }
                }

                return true;
            }
        }, ttl * 4));
    }
}
