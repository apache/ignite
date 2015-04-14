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

package org.apache.ignite.internal.processors.cache.multijvm;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.distributed.near.*;
import org.apache.ignite.internal.processors.resource.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.*;

import java.util.*;

/**
 * TODO: Add class description.
 */
public class GridCachePartitionedMultiJvmFullApiSelfTest extends GridCachePartitionedMultiNodeFullApiSelfTest {
    /** VM ip finder for TCP discovery. */
    public static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryMulticastIpFinder(){{
        setAddresses(Collections.singleton("127.0.0.1:47500..47509"));
    }};

    /** Proces name to process map. */
    private final Map<String, IgniteProcessProxy> ignites = new HashMap<>();

    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
//        IgniteConfiguration cfg = super.getConfiguration(gridName);
//
//        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        return IgniteNodeRunner.configuration(null); // TODO: change.
    }

    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        return super.cacheConfiguration(gridName); // TODO: CODE: implement.
    }

    @Override protected int gridCount() {
        return 1;
    }

    protected boolean isMultiJvm() {
        return true;
    }

    @Override protected void afterTestsStopped() throws Exception {
        for (IgniteProcessProxy ignite : ignites.values())
            ignite.getProcess().kill();

        ignites.clear();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    protected Ignite startGrid(String gridName, GridSpringResourceContext ctx) throws Exception {
        startingGrid.set(gridName);

        try {
            IgniteConfiguration cfg = optimize(getConfiguration(gridName));

            IgniteProcessProxy proxy = new IgniteProcessProxy(cfg, log);
            
            ignites.put(gridName, proxy);
            
            return proxy;
        }
        finally {
            startingGrid.set(null);
        }
    }

    @Override protected IgniteEx grid(int idx) {
        String name = getTestGridName(idx);
        
        return ignites.get(name);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllRemoveAll() throws Exception {
//        for (int i = 0; i < gridCount(); i++) {
//            IgniteEx grid0 = grid0(i);
//
//            info(">>>>> Grid" + i + ": " + grid0.localNode().id());
//        }

        Map<Integer, Integer> putMap = new LinkedHashMap<>();

        int size = 100;

        for (int i = 0; i < size; i++)
            putMap.put(i, i * i);

        IgniteEx grid0 = grid(0);
        
        IgniteCache<Object, Object> c0 = grid0.cache(null);
        
        IgniteEx grid1 = grid(1);
        
        IgniteCache<Object, Object> c1 = grid1.cache(null);

        c0.putAll(putMap);

        atomicClockModeDelay(c0);

        c1.removeAll(putMap.keySet());

        for (int i = 0; i < size; i++) {
            assertNull(c0.get(i));
            assertNull(c1.get(i));
        }
    }
}
