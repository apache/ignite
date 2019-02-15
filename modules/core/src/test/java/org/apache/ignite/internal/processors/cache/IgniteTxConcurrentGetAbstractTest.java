/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cache;

import java.util.UUID;
import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheAdapter;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Checks multithreaded put/get cache operations on one node.
 */
public abstract class IgniteTxConcurrentGetAbstractTest extends GridCommonAbstractTest {
    /** Debug flag. */
    private static final boolean DEBUG = false;

    /** */
    private static final int THREAD_NUM = 20;

    /**
     * Default constructor.
     *
     */
    protected IgniteTxConcurrentGetAbstractTest() {
        super(true /** Start grid. */);
    }

    /**
     * @param g Grid.
     * @return Near cache.
     */
    GridNearCacheAdapter<String, Integer> near(Ignite g) {
        return (GridNearCacheAdapter<String, Integer>)((IgniteKernal)g).<String, Integer>internalCache(DEFAULT_CACHE_NAME);
    }

    /**
     * @param g Grid.
     * @return DHT cache.
     */
    GridDhtCacheAdapter<String, Integer> dht(Ignite g) {
        return near(g).dht();
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPutGet() throws Exception {
        // Random key.
        final String key = UUID.randomUUID().toString();

        final Ignite ignite = grid();

        ignite.cache(DEFAULT_CACHE_NAME).put(key, "val");

        GridCacheEntryEx dhtEntry = dht(ignite).peekEx(key);

        if (DEBUG)
            info("DHT entry [hash=" + System.identityHashCode(dhtEntry) + ", entry=" + dhtEntry + ']');

        String val = txGet(ignite, key);

        assertNotNull(val);

        info("Starting threads: " + THREAD_NUM);

        multithreaded(new Callable<String>() {
            @Override public String call() throws Exception {
                return txGet(ignite, key);
            }
        }, THREAD_NUM, "getter-thread");
    }

    /**
     * @param ignite Grid.
     * @param key Key.
     * @return Value.
     * @throws Exception If failed.
     */
    private String txGet(Ignite ignite, String key) throws Exception {
        try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            GridCacheEntryEx dhtEntry = dht(ignite).peekEx(key);

            if (DEBUG)
                info("DHT entry [hash=" + System.identityHashCode(dhtEntry) + ", xid=" + tx.xid() +
                    ", entry=" + dhtEntry + ']');

            String val = ignite.<String, String>cache(DEFAULT_CACHE_NAME).get(key);

            assertNotNull(val);
            assertEquals("val", val);

            tx.commit();

            return val;
        }
    }
}
