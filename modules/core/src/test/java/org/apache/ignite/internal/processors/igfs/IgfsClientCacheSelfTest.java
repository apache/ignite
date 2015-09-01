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

package org.apache.ignite.internal.processors.igfs;

import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.igfs.IgfsGroupDataBlocksKeyMapper;
import org.apache.ignite.igfs.IgfsMode;
import org.apache.ignite.igfs.secondary.IgfsSecondaryFileSystem;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 * Test for igfs with nodes in client mode (see {@link IgniteConfiguration#setClientMode(boolean)}.
 */
public class IgfsClientCacheSelfTest extends IgfsAbstractSelfTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Meta-information cache name. */
    private static final String META_CACHE_NAME = "meta";

    /** Data cache name. */
    private static final String DATA_CACHE_NAME = null;

    /** Regular cache name. */
    private static final String CACHE_NAME = "cache";

    /**
     * Constructor.
     */
    public IgfsClientCacheSelfTest() {
        super(IgfsMode.PRIMARY);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        igfsSecondaryFileSystem = createSecondaryFileSystemStack();

        Ignite ignite1 = G.start(getConfiguration(getTestGridName(1)));

        igfs = (IgfsImpl) ignite1.fileSystem("igfs");
    }

    /**{@inheritDoc} */
    protected IgfsSecondaryFileSystem createSecondaryFileSystemStack() throws Exception {
        Ignite igniteSecondary = G.start(getConfiguration(getTestGridName(0)));

        IgfsEx secondaryIgfsImpl = (IgfsEx)igniteSecondary.fileSystem("igfs");

        igfsSecondary = new IgfsExUniversalFileSystemAdapter(secondaryIgfsImpl);

        return secondaryIgfsImpl.asSecondary();
    }

    /**
     *
     * @param gridName Grid name.
     * @return Ignite configuration.
     * @throws Exception If failed.
     */
    protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCacheConfiguration(cacheConfiguration(META_CACHE_NAME), cacheConfiguration(DATA_CACHE_NAME),
            cacheConfiguration(CACHE_NAME));

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        if (!gridName.equals(getTestGridName(0))) {
            cfg.setClientMode(true);

            disco.setForceServerMode(true);
        }

        cfg.setDiscoverySpi(disco);

        FileSystemConfiguration igfsCfg = new FileSystemConfiguration();

        igfsCfg.setMetaCacheName(META_CACHE_NAME);
        igfsCfg.setDataCacheName(DATA_CACHE_NAME);
        igfsCfg.setName("igfs");

        cfg.setFileSystemConfiguration(igfsCfg);

        return cfg;
    }

    /**
     * @param cacheName Cache name.
     * @return Cache configuration.
     */
    protected CacheConfiguration cacheConfiguration(String cacheName) {
        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setName(cacheName);

        cacheCfg.setNearConfiguration(null);

        if (META_CACHE_NAME.equals(cacheName))
            cacheCfg.setCacheMode(REPLICATED);
        else {
            cacheCfg.setCacheMode(PARTITIONED);

            cacheCfg.setBackups(0);
            cacheCfg.setAffinityMapper(new IgfsGroupDataBlocksKeyMapper(128));
        }

        cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);

        return cacheCfg;
    }
}