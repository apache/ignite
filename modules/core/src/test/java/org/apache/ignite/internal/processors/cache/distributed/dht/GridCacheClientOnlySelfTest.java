/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.distributed.dht;

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheClientModesAbstractSelfTest;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 * Client only test.
 */
@SuppressWarnings("RedundantMethodOverride")
public abstract class GridCacheClientOnlySelfTest extends GridCacheClientModesAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected NearCacheConfiguration nearConfiguration() {
        return null;
    }

    /** */
    public static class CaseReplicatedAtomic extends GridCacheClientOnlySelfTest {
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
    public static class CaseReplicatedTransactional extends GridCacheClientOnlySelfTest {
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
    public static class CasePartitionedAtomic extends GridCacheClientOnlySelfTest {
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
    public static class CasePartitionedTransactional extends GridCacheClientOnlySelfTest {
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