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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.common.*;

import java.util.concurrent.*;

/**
 *
 */
public class IgniteDynamicCacheAndNodeStop extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheAndNodeStop() throws Exception {
        final Ignite ignite = startGrid(0);

        for (int i = 0; i < 3; i++) {
            log.info("Iteration: " + i);

            startGrid(1);

            final  CacheConfiguration ccfg = new CacheConfiguration();

            ignite.createCache(ccfg);

            final CyclicBarrier barrier = new CyclicBarrier(2);

            IgniteInternalFuture<?> fut1 = GridTestUtils.runAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    barrier.await();

                    ignite.destroyCache(null);

                    return null;
                }
            });

            IgniteInternalFuture<?> fut2 = GridTestUtils.runAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    barrier.await();

                    stopGrid(1);

                    return null;
                }
            });

            fut1.get();
            fut2.get();
        }
    }
}
