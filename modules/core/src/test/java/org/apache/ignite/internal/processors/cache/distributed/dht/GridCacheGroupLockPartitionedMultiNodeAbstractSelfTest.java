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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.transactions.*;

import java.util.*;

import static org.apache.ignite.transactions.IgniteTxConcurrency.*;
import static org.apache.ignite.transactions.IgniteTxIsolation.*;

/**
 * Multi-node test for group locking.
 */
public abstract class GridCacheGroupLockPartitionedMultiNodeAbstractSelfTest extends
    GridCacheGroupLockPartitionedAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonLocalKeyOptimistic() throws Exception {
        checkNonLocalKey(OPTIMISTIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNonLocalKeyPessimistic() throws Exception {
        checkNonLocalKey(PESSIMISTIC);
    }

    /**
     * @throws Exception If failed.
     */
    private void checkNonLocalKey(IgniteTxConcurrency concurrency) throws Exception {
        final UUID key = primaryKeyForCache(grid(1));

        IgniteCache<Object, Object> cache = grid(0).jcache(null);

        IgniteTx tx = null;
        try {
            tx = grid(0).transactions().txStartAffinity(null, key, concurrency, READ_COMMITTED, 0, 2);

            cache.put(new CacheAffinityKey<>("1", key), "2");

            tx.commit();

            fail("Exception should be thrown.");
        }
        catch (IgniteException ignored) {
            // Expected exception.
        }
        finally {
            if (tx != null)
                tx.close();

            assertNull(grid(0).transactions().tx());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testNearReadersUpdateWithAffinityReaderOptimistic() throws Exception {
        checkNearReadersUpdate(true, OPTIMISTIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNearReadersUpdateWithAffinityReaderPessimistic() throws Exception {
        checkNearReadersUpdate(true, PESSIMISTIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNearReaderUpdateWithoutAffinityReaderOptimistic() throws Exception {
        checkNearReadersUpdate(false, OPTIMISTIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNearReaderUpdateWithoutAffinityReaderPessimistic() throws Exception {
        checkNearReadersUpdate(false, PESSIMISTIC);
    }

    /**
     * @throws Exception If failed.
     */
    private void checkNearReadersUpdate(boolean touchAffKey, IgniteTxConcurrency concurrency) throws Exception {
        UUID affinityKey = primaryKeyForCache(grid(0));

        CacheAffinityKey<String> key1 = new CacheAffinityKey<>("key1", affinityKey);
        CacheAffinityKey<String> key2 = new CacheAffinityKey<>("key2", affinityKey);
        CacheAffinityKey<String> key3 = new CacheAffinityKey<>("key3", affinityKey);

        grid(0).jcache(null).put(affinityKey, "aff");

        IgniteCache<CacheAffinityKey<String>, String> cache = grid(0).jcache(null);

        cache.putAll(F.asMap(
            key1, "val1",
            key2, "val2",
            key3, "val3")
        );

        Ignite reader = null;

        for (int i = 0; i < gridCount(); i++) {
            if (!grid(i).affinity(null).isPrimaryOrBackup(grid(i).localNode(), affinityKey))
                reader = grid(i);
        }

        assert reader != null;

        info(">>> Reader is " + reader.cluster().localNode().id());

        // Add reader.
        if (touchAffKey)
            assertEquals("aff", reader.jcache(null).get(affinityKey));

        assertEquals("val1", reader.jcache(null).get(key1));
        assertEquals("val2", reader.jcache(null).get(key2));
        assertEquals("val3", reader.jcache(null).get(key3));

        if (nearEnabled()) {
            assertEquals("val1", reader.jcache(null).localPeek(key1, CachePeekMode.ONHEAP));
            assertEquals("val2", reader.jcache(null).localPeek(key2, CachePeekMode.ONHEAP));
            assertEquals("val3", reader.jcache(null).localPeek(key3, CachePeekMode.ONHEAP));
        }

        try (IgniteTx tx = grid(0).transactions()
            .txStartAffinity(null, affinityKey, concurrency, READ_COMMITTED, 0, 3)) {
            cache.putAll(F.asMap(
                key1, "val01",
                key2, "val02",
                key3, "val03")
            );

            tx.commit();
        }

        if (nearEnabled()) {
            assertEquals("val01", reader.jcache(null).localPeek(key1, CachePeekMode.ONHEAP));
            assertEquals("val02", reader.jcache(null).localPeek(key2, CachePeekMode.ONHEAP));
            assertEquals("val03", reader.jcache(null).localPeek(key3, CachePeekMode.ONHEAP));
        }
    }
}
