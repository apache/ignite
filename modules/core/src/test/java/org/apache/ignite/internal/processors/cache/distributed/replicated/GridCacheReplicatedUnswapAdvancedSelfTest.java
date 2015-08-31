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

import java.io.Serializable;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.swapspace.file.FileSwapSpaceSpi;
import org.apache.ignite.testframework.GridTestClassLoader;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT;

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