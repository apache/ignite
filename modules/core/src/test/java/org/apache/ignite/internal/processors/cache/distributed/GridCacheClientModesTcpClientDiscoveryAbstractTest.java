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

package org.apache.ignite.internal.processors.cache.distributed;

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 * Tests {@link org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi} in client mode.
 */
@SuppressWarnings("RedundantMethodOverride")
public abstract class GridCacheClientModesTcpClientDiscoveryAbstractTest extends GridCacheClientModesAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected boolean isClientStartedLast() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setForceServerMode(false);

        return cfg;
    }

    /** */
    public static class CaseNearReplicatedAtomic extends GridCacheClientModesTcpClientDiscoveryAbstractTest {
        /** {@inheritDoc} */
        @Override protected CacheMode cacheMode() {
            return REPLICATED;
        }

        /** {@inheritDoc} */
        @Override protected CacheAtomicityMode atomicityMode() {
            return CacheAtomicityMode.ATOMIC;
        }
    }

    /** */
    public static class CaseNearReplicatedTransactional extends GridCacheClientModesTcpClientDiscoveryAbstractTest {
        /** {@inheritDoc} */
        @Override protected CacheMode cacheMode() {
            return REPLICATED;
        }

        /** {@inheritDoc} */
        @Override protected CacheAtomicityMode atomicityMode() {
            return CacheAtomicityMode.TRANSACTIONAL;
        }
    }

    /** */
    public static class CaseNearPartitionedAtomic extends GridCacheClientModesTcpClientDiscoveryAbstractTest {
        /** {@inheritDoc} */
        @Override protected CacheMode cacheMode() {
            return PARTITIONED;
        }

        /** {@inheritDoc} */
        @Override protected CacheAtomicityMode atomicityMode() {
            return CacheAtomicityMode.ATOMIC;
        }
    }

    /** */
    public static class CaseNearPartitionedTransactional extends GridCacheClientModesTcpClientDiscoveryAbstractTest {
        /** {@inheritDoc} */
        @Override protected CacheMode cacheMode() {
            return PARTITIONED;
        }

        /** {@inheritDoc} */
        @Override protected CacheAtomicityMode atomicityMode() {
            return CacheAtomicityMode.TRANSACTIONAL;
        }
    }

    /** */
    public static class CaseClientReplicatedAtomic extends GridCacheClientModesTcpClientDiscoveryAbstractTest {
        /** {@inheritDoc} */
        @Override protected NearCacheConfiguration nearConfiguration() {
            return null;
        }

        /** {@inheritDoc} */
        @Override protected CacheMode cacheMode() {
            return REPLICATED;
        }

        /** {@inheritDoc} */
        @Override protected CacheAtomicityMode atomicityMode() {
            return CacheAtomicityMode.ATOMIC;
        }
    }

    /** */
    public static class CaseClientReplicatedTransactional extends GridCacheClientModesTcpClientDiscoveryAbstractTest {
        /** {@inheritDoc} */
        @Override protected NearCacheConfiguration nearConfiguration() {
            return null;
        }

        /** {@inheritDoc} */
        @Override protected CacheMode cacheMode() {
            return REPLICATED;
        }

        /** {@inheritDoc} */
        @Override protected CacheAtomicityMode atomicityMode() {
            return CacheAtomicityMode.TRANSACTIONAL;
        }
    }

    /** */
    public static class CaseClientPartitionedAtomic extends GridCacheClientModesTcpClientDiscoveryAbstractTest {
        /** {@inheritDoc} */
        @Override protected NearCacheConfiguration nearConfiguration() {
            return null;
        }

        /** {@inheritDoc} */
        @Override protected CacheMode cacheMode() {
            return PARTITIONED;
        }

        /** {@inheritDoc} */
        @Override protected CacheAtomicityMode atomicityMode() {
            return CacheAtomicityMode.ATOMIC;
        }
    }

    /** */
    public static class CaseClientPartitionedTransactional extends GridCacheClientModesTcpClientDiscoveryAbstractTest {
        /** {@inheritDoc} */
        @Override protected NearCacheConfiguration nearConfiguration() {
            return null;
        }

        /** {@inheritDoc} */
        @Override protected CacheMode cacheMode() {
            return PARTITIONED;
        }

        /** {@inheritDoc} */
        @Override protected CacheAtomicityMode atomicityMode() {
            return CacheAtomicityMode.TRANSACTIONAL;
        }
    }
}