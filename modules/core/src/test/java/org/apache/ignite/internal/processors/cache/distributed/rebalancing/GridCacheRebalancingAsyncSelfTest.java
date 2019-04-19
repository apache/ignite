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

package org.apache.ignite.internal.processors.cache.distributed.rebalancing;

import java.util.Collections;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemander;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TestTcpDiscoverySpi;
import org.junit.Test;

/**
 *
 */
public class GridCacheRebalancingAsyncSelfTest extends GridCacheRebalancingSyncSelfTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration iCfg = super.getConfiguration(igniteInstanceName);

        for (CacheConfiguration cacheCfg : iCfg.getCacheConfiguration())
            cacheCfg.setRebalanceMode(CacheRebalanceMode.ASYNC);

        return iCfg;
    }

    /**
     * @throws Exception Exception.
     */
    @Test
    public void testNodeFailedAtRebalancing() throws Exception {
        IgniteEx ignite = startGrid(0);

        generateData(ignite, 0, 0);

        log.info("Preloading started.");

        startGrid(1);

        GridDhtPartitionDemander.RebalanceFuture fut = (GridDhtPartitionDemander.RebalanceFuture)grid(1).context().
            cache().internalCache(CACHE_NAME_DHT_REPLICATED).preloader().rebalanceFuture();

        fut.get();

        U.sleep(10);

        ((TestTcpDiscoverySpi)grid(1).configuration().getDiscoverySpi()).simulateNodeFailure();

        awaitPartitionMapExchange(false, false, Collections.singletonList(ignite.localNode()));

        checkSupplyContextMapIsEmpty();
    }
}
