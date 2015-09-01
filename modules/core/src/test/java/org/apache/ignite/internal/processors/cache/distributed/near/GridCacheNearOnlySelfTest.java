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

package org.apache.ignite.internal.processors.cache.distributed.near;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheClientModesAbstractSelfTest;

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