/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.distributed.rebalancing;

import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;

/**
 *
 */
public class GridCacheRebalancingAsyncSelfTest extends GridCacheRebalancingSyncSelfTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration iCfg = super.getConfiguration(gridName);

        for (CacheConfiguration cacheCfg : iCfg.getCacheConfiguration()) {
            cacheCfg.setRebalanceMode(CacheRebalanceMode.ASYNC);
        }

        iCfg.setDiscoverySpi(new FailableTcpDiscoverySpi());

        ((TcpDiscoverySpi)iCfg.getDiscoverySpi()).setIpFinder(ipFinder);
        ((TcpDiscoverySpi)iCfg.getDiscoverySpi()).setForceServerMode(true);

        if (getTestGridName(20).equals(gridName))
            spi = (FailableTcpDiscoverySpi)iCfg.getDiscoverySpi();

        return iCfg;
    }

    public static class FailableTcpDiscoverySpi extends TcpDiscoverySpi {
        public void fail() {
            simulateNodeFailure();
        }
    }

    private volatile FailableTcpDiscoverySpi spi;

    /**
     * @throws Exception
     */
    public void testNodeFailedAtRebalancing() throws Exception {
        Ignite ignite = startGrid(0);

        generateData(ignite);

        log.info("Preloading started.");

        startGrid(1);

        waitForRebalancing(1, 2);

        startGrid(20);

        waitForRebalancing(20, 3);

        spi.fail();

        waitForRebalancing(0, 4);
        waitForRebalancing(1, 4);

        stopAllGrids();
    }
}