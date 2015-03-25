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

package org.apache.ignite.internal.processors.cache.distributed.replicated;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.spi.swapspace.file.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.common.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.events.EventType.*;

/**
 * Advanced promote test for replicated cache.
 */
public class GridCacheReplicatedUnswapAdvancedSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setPeerClassLoadingLocalClassPathExclude(GridCacheReplicatedUnswapAdvancedSelfTest.class.getName(),
            TestClass.class.getName());

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(REPLICATED);
        cacheCfg.setSwapEnabled(true);

        cfg.setCacheConfiguration(cacheCfg);

        cfg.setSwapSpaceSpi(new FileSwapSpaceSpi());

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testUnswapAdvanced() throws Exception {
        Ignite g1 = startGrid(1);
        Ignite g2 = startGrid(2);

        assert g1.cluster().nodes().size() > 1 : "This test needs at least two grid nodes started.";

        IgniteCache<Object, Object> cache1 = g1.cache(null);
        IgniteCache<Object, Object> cache2 = g2.cache(null);

        try {
            ClassLoader ldr = new GridTestClassLoader(
                GridCacheReplicatedUnswapAdvancedSelfTest.class.getName(),
                TestClass.class.getName());

            Object v = ldr.loadClass(TestClass.class.getName()).newInstance();

            info("v loader: " + v.getClass().getClassLoader());

            final CountDownLatch putLatch = new CountDownLatch(1);

            g2.events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    assert evt.type() == EVT_CACHE_OBJECT_PUT;

                    putLatch.countDown();

                    return true;
                }
            }, EVT_CACHE_OBJECT_PUT);

            String key = null;

            for (int i = 0; i < 1000; i++) {
                String k = "key-" + i;

                if (affinity(cache1).isPrimary(g1.cluster().localNode(), k)) {
                    key = k;

                    break;
                }
            }

            assertNotNull(key);

            // Put value into cache of the first grid.
            cache1.put(key, v);

            assert putLatch.await(10, SECONDS);

            assert cache2.containsKey(key);

            Object v2 = cache2.get(key);

            info("v2 loader: " + v2.getClass().getClassLoader());

            assert v2 != null;
            assert v2.toString().equals(v.toString());
            assert !v2.getClass().getClassLoader().equals(getClass().getClassLoader());
            assert v2.getClass().getClassLoader().getClass().getName().contains("GridDeploymentClassLoader");

            // To swap storage.
            cache2.localEvict(Collections.<Object>singleton(key));

            cache2.localPromote(Collections.singleton(key));

            v2 = cache2.localPeek(key, CachePeekMode.ONHEAP);

            log.info("Unswapped entry value: " + v2);

            assert v2 != null;

            assert v2.getClass().getClassLoader().getClass().getName().contains("GridDeploymentClassLoader");
        }
        finally {
            stopGrid(1);
            stopGrid(2);
        }
    }
    /**
     * Test class.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class TestClass implements Serializable {
        /** String value. */
        private String s = "Test string";

        /**
         * @return String value.
         */
        public String getStr() {
            return s;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(TestClass.class, this);
        }
    }
}
