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
import org.apache.ignite.marshaller.optimized.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.cache.*;
import org.gridgain.testframework.junits.common.*;

/**
 *
 */
public class IgniteCacheTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /**
     * @return Grids count to start.
     */
    protected int gridCount() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        int cnt = gridCount();

        assert cnt >= 1 : "At least one grid must be started";

        startGridsMultiThreaded(cnt);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setMaxMissedHeartbeats(Integer.MAX_VALUE);

        disco.setIpFinder(ipFinder);

        if (isDebug())
            disco.setAckTimeout(Integer.MAX_VALUE);

        cfg.setDiscoverySpi(disco);

        cfg.setCacheConfiguration(cacheConfiguration(gridName));

        cfg.setMarshaller(new IgniteOptimizedMarshaller(false));

        return cfg;
    }

    /**
     * @param gridName Grid name.
     * @return Cache configuration.
     * @throws Exception In case of error.
     */
    protected GridCacheConfiguration cacheConfiguration(String gridName) throws Exception {
        return defaultCacheConfiguration();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutGetRemove() throws Exception {
        IgniteCache<Integer, String> cache = jcache();

        for (int i = 0; i < 10; i++)
            cache.put(i, String.valueOf(i));

        for (int i = 0; i < 10; i++)
            assertEquals(String.valueOf(i), cache.get(i));

        for (int i = 0; i < 10; i++)
            cache.remove(i);

        for (int i = 0; i < 10; i++)
            assertNull(cache.get(i));
    }

    /**
     * @return Cache.
     */
    protected <K, V> IgniteCache<K, V> jcache() {
        return jcache(0);
    }

    /**
     * @param idx Grid index.
     * @return Cache.
     */
    protected <K, V> IgniteCache<K, V> jcache(int idx) {
        return grid(idx).jcache(null);
    }
}
