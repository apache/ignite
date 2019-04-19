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

import java.util.Collection;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.cache.IgniteTxReentryAbstractSelfTest;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 *
 */
public class IgniteTxReentryColocatedSelfTest extends IgniteTxReentryAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected int testKey() {
        int key = 0;

        IgniteCache<Object, Object> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        while (true) {
            Collection<ClusterNode> nodes = affinity(cache).mapKeyToPrimaryAndBackups(key);

            if (nodes.contains(grid(0).localNode()))
                key++;
            else
                break;
        }

        return key;
    }

    /** {@inheritDoc} */
    @Override protected int expectedNearLockRequests() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected int expectedDhtLockRequests() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override protected int expectedDistributedLockRequests() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected boolean nearEnabled() {
        return false;
    }
}