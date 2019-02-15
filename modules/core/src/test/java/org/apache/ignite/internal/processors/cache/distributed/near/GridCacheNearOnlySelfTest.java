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

package org.apache.ignite.internal.processors.cache.distributed.near;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheClientModesAbstractSelfTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 * Near only self test.
 */
@SuppressWarnings("RedundantMethodOverride")
public abstract class GridCacheNearOnlySelfTest extends GridCacheClientModesAbstractSelfTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUpdateNearOnlyReader() throws Exception {
        IgniteCache<Object, Object> dhtCache = dhtCache();

        final int keyCnt = 100;

        for (int i = 0; i < keyCnt; i++)
            dhtCache.put(i, i);

        IgniteCache<Object, Object> nearOnlyCache = nearOnlyCache();

        for (int i = 0; i < keyCnt; i++) {
            assertNull(nearOnlyCache.localPeek(i, CachePeekMode.ONHEAP));

            assertEquals(i, nearOnlyCache.get(i));
            assertEquals(i, nearOnlyCache.localPeek(i, CachePeekMode.ONHEAP));
        }

        for (int i = 0; i < keyCnt; i++)
            dhtCache.put(i, i * i);

        for (int i = 0; i < keyCnt; i++) {
            assertEquals(i * i, nearOnlyCache.localPeek(i, CachePeekMode.ONHEAP));

            assertEquals(i * i, nearOnlyCache.get(i));
        }
    }

    /** */
    public static class CaseReplicatedAtomic extends GridCacheNearOnlySelfTest {
        /** {@inheritDoc} */
        @Override protected CacheMode cacheMode() {
            return REPLICATED;
        }

        /** {@inheritDoc} */
        @Override protected CacheAtomicityMode atomicityMode() {
            return ATOMIC;
        }
    }

    /** */
    public static class CaseReplicatedTransactional extends GridCacheNearOnlySelfTest {
        /** {@inheritDoc} */
        @Override protected CacheMode cacheMode() {
            return REPLICATED;
        }

        /** {@inheritDoc} */
        @Override protected CacheAtomicityMode atomicityMode() {
            return TRANSACTIONAL;
        }
    }

    /** */
    public static class CasePartitionedAtomic extends GridCacheNearOnlySelfTest {
        /** {@inheritDoc} */
        @Override protected CacheMode cacheMode() {
            return PARTITIONED;
        }

        /** {@inheritDoc} */
        @Override protected CacheAtomicityMode atomicityMode() {
            return ATOMIC;
        }
    }

    /** */
    public static class CasePartitionedTransactional extends GridCacheNearOnlySelfTest {
        /** {@inheritDoc} */
        @Override protected CacheMode cacheMode() {
            return PARTITIONED;
        }

        /** {@inheritDoc} */
        @Override protected CacheAtomicityMode atomicityMode() {
            return TRANSACTIONAL;
        }
    }
}
