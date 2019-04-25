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

package org.apache.ignite.internal.processors.cache.distributed.near;

import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheRebalanceMode.NONE;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;

/**
 * Test none rebalance mode.
 */
public class NoneRebalanceModeSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(igniteInstanceName);

        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setRebalanceMode(NONE);

        c.setCacheConfiguration(cc);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(0);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRemoveAll() throws Exception {
        GridNearTransactionalCache cache = (GridNearTransactionalCache)((IgniteKernal)grid(0)).internalCache(DEFAULT_CACHE_NAME);

        for (GridDhtLocalPartition part : cache.dht().topology().localPartitions())
            assertEquals(OWNING, part.state());

        grid(0).cache(DEFAULT_CACHE_NAME).removeAll();
    }
}
