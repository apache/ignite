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

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;

import static org.apache.ignite.cache.GridCacheMode.*;
import static org.apache.ignite.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Test for {@link org.apache.ignite.cache.CacheConfiguration#isStoreValueBytes()}.
 */
public class GridCacheStoreValueBytesSelfTest extends GridCommonAbstractTest {
    /** */
    private boolean storeValBytes;

    /** VM ip finder for TCP discovery. */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        CacheConfiguration ccfg = defaultCacheConfiguration();

        ccfg.setCacheMode(REPLICATED);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setStoreValueBytes(storeValBytes);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testDisabled() throws Exception {
        storeValBytes = false;

        Ignite g0 = startGrid(0);
        Ignite g1 = startGrid(1);

        GridCache<Integer, String> c = g0.cache(null);

        c.put(1, "Cached value");

        GridCacheEntryEx<Object, Object> entry = ((GridKernal)g1).internalCache().peekEx(1);

        assert entry != null;
        assert entry.valueBytes().isNull();
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testEnabled() throws Exception {
        storeValBytes = true;

        Ignite g0 = startGrid(0);
        Ignite g1 = startGrid(1);

        GridCache<Integer, String> c = g0.cache(null);

        c.put(1, "Cached value");

        GridCacheEntryEx<Object, Object> entry = ((GridKernal)g1).internalCache().peekEx(1);

        assert entry != null;
        assert entry.valueBytes() != null;
    }
}
