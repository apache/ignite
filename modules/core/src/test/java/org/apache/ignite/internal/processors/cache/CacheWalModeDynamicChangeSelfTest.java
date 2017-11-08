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

package org.apache.ignite.internal.processors.cache;

import java.io.File;
import java.nio.file.Paths;
import java.util.Collections;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;

/**
 * Checks stop and destroy methods behavior.
 */
@SuppressWarnings("unchecked")
public class CacheWalModeDynamicChangeSelfTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Cache 1. */
    private static final String CACHE1 = "cache1";

    /** Cache 2. */
    private static final String CACHE2 = "cache2";

    /** Cache 3. */
    private static final String CACHE3 = "cache3";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration iCfg = super.getConfiguration(igniteInstanceName);

        if (getTestIgniteInstanceName(3).equals(igniteInstanceName))
            iCfg.setClientMode(true);

        ((TcpDiscoverySpi)iCfg.getDiscoverySpi()).setIpFinder(ipFinder);
        ((TcpDiscoverySpi)iCfg.getDiscoverySpi()).setForceServerMode(true);

        CacheConfiguration cache1 = new CacheConfiguration(CACHE1);
        CacheConfiguration cache2 = new CacheConfiguration(CACHE2).setGroupName("group");
        CacheConfiguration cache3 = new CacheConfiguration(CACHE3).setGroupName("group");

        iCfg.setCacheConfiguration(cache1, cache2, cache3);

        iCfg.setPersistentStoreConfiguration(new PersistentStoreConfiguration());

        return iCfg;
    }

    /**
     *
     */
    public void testWalEnablingDisabling() throws Exception {
        // Removing persistence data
        U.delete(Paths.get(U.defaultWorkDirectory() + File.separator + DFLT_STORE_DIR).toFile());

        Ignite ignite1 = startGrid(1);
        Ignite ignite2 = startGrid(2);
        Ignite client = startGrid(3);

        client.active(true);

        GridCacheSharedContext ctx1 = ((IgniteKernal)ignite1).context().cache().context();
        GridCacheSharedContext ctx2 = ((IgniteKernal)ignite2).context().cache().context();

        final int g1 = ctx1.cache().cacheDescriptor(CACHE1).groupId();
        final int g2 = ctx1.cache().cacheDescriptor(CACHE2).groupId();
        final int g3 = ctx1.cache().cacheDescriptor(CACHE3).groupId();

        assertTrue(ctx1.wal().enabled(g1));
        assertTrue(ctx1.wal().enabled(g2));
        assertTrue(ctx1.wal().enabled(g3));

        client.disableWal(Collections.singleton(CACHE1));

        assertFalse(ctx1.wal().enabled(g1));
        assertTrue(ctx1.wal().enabled(g2));
        assertTrue(ctx1.wal().enabled(g3));

        IgniteCache cache1 = client.getOrCreateCache(CACHE1);

        int size = 100_000;

        for (int i = 0; i < size; i++)
            cache1.put(i, i);

        assertEquals(size, cache1.size());

        client.enableWal(Collections.singleton(CACHE1));

        // Making sure there is no dirty pages after WAL enabling
        // TODO check only cache's region
        for (DataRegion memPlc : ctx1.database().dataRegions())
            assertTrue(((PageMemoryEx)memPlc.pageMemory()).beginCheckpoint().isEmpty());

        for (DataRegion memPlc : ctx2.database().dataRegions())
            assertTrue(((PageMemoryEx)memPlc.pageMemory()).beginCheckpoint().isEmpty());

        client.disableWal(Collections.singleton(CACHE2));

        assertTrue(ctx1.wal().enabled(g1));
        assertFalse(ctx1.wal().enabled(g2));
        assertFalse(ctx1.wal().enabled(g3));
    }
}
