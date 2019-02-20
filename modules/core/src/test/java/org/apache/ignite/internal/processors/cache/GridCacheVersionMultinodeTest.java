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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheEntry;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.jetbrains.annotations.Nullable;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
public class GridCacheVersionMultinodeTest extends GridCacheAbstractSelfTest {
    /** */
    private CacheAtomicityMode atomicityMode;

    /** */
    private boolean near;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String igniteInstanceName) throws Exception {
        CacheConfiguration ccfg = super.cacheConfiguration(igniteInstanceName);

        assert atomicityMode != null;

        ccfg.setAtomicityMode(atomicityMode);

        ccfg.setNearConfiguration(near ? new NearCacheConfiguration() : null);

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testVersionTx() throws Exception {
        atomicityMode = TRANSACTIONAL;

        checkVersion();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testVersionTxNearEnabled() throws Exception {
        atomicityMode = TRANSACTIONAL;

        near = true;

        checkVersion();
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-8582")
    @Test
    public void testVersionMvccTx() throws Exception {
        atomicityMode = TRANSACTIONAL_SNAPSHOT;

        checkVersion();
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-7187")
    @Test
    public void testVersionMvccTxNearEnabled() throws Exception {
        atomicityMode = TRANSACTIONAL_SNAPSHOT;

        near = true;

        checkVersion();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testVersionAtomicPrimary() throws Exception {
        atomicityMode = ATOMIC;

        checkVersion();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testVersionAtomicPrimaryNearEnabled() throws Exception {
        atomicityMode = ATOMIC;

        near = true;

        checkVersion();
    }

    /**
     * @throws Exception If failed.
     */
    private void checkVersion() throws Exception {
        super.beforeTestsStarted();

        for (int i = 0; i < 100; i++) {
            checkVersion(String.valueOf(i), null); // Create.

            checkVersion(String.valueOf(i), null); // Update.
        }

        if (atomicityMode != ATOMIC) {
            for (int i = 100; i < 200; i++) {
                checkVersion(String.valueOf(i), PESSIMISTIC); // Create.

                checkVersion(String.valueOf(i), PESSIMISTIC); // Update.
            }

            if (atomicityMode != TRANSACTIONAL_SNAPSHOT) {
                for (int i = 200; i < 300; i++) {
                    checkVersion(String.valueOf(i), OPTIMISTIC); // Create.

                    checkVersion(String.valueOf(i), OPTIMISTIC); // Update.
                }
            }
        }
    }

    /**
     * @param key Key.
     * @param txMode Non null tx mode if explicit transaction should be started.
     * @throws Exception If failed.
     */
    private void checkVersion(String key, @Nullable TransactionConcurrency txMode) throws Exception {
        IgniteCache<String, Integer> cache = jcache(0);

        Transaction tx = null;

        if (txMode != null)
            tx = cache.unwrap(Ignite.class).transactions().txStart(txMode, REPEATABLE_READ);

        try {
            cache.put(key, 1);

            if (tx != null)
                tx.commit();
        }
        finally {
            if (tx != null)
                tx.close();
        }

        checkEntryVersion(key);
    }

    /**
     * @param key Key.
     * @throws Exception If failed.
     */
    private void checkEntryVersion(String key) throws Exception {
        GridCacheVersion ver = null;

        boolean verified = false;

        for (int i = 0; i < gridCount(); i++) {
            IgniteKernal grid = (IgniteKernal)grid(i);

            GridCacheAdapter<Object, Object> cache = grid.context().cache().internalCache(DEFAULT_CACHE_NAME);

            GridCacheEntryEx e;

            if (cache.affinity().isPrimaryOrBackup(grid.localNode(), key)) {
                if (cache instanceof GridNearCacheAdapter)
                    cache = ((GridNearCacheAdapter<Object, Object>)cache).dht();

                e = cache.entryEx(key);

                e.unswap();

                assertNotNull(e.rawGet());
            }
            else
                e = cache.peekEx(key);

            if (e != null) {
                if (ver != null) {
                    assertEquals("Non-equal versions for key: " + key,
                        ver,
                        e instanceof GridNearCacheEntry ? ((GridNearCacheEntry)e).dhtVersion() : e.version());

                    verified = true;
                }
                else
                    ver = e instanceof GridNearCacheEntry ? ((GridNearCacheEntry)e).dhtVersion() : e.version();
            }
        }

        assertTrue(verified);
    }
}
