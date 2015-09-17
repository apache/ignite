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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.igfs.IgfsGroupDataBlocksKeyMapper;
import org.apache.ignite.internal.processors.cache.GridCacheDefaultAffinityKeyMapper;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.igfs.IgfsMode.DUAL_ASYNC;
import static org.apache.ignite.igfs.IgfsMode.DUAL_SYNC;
import static org.apache.ignite.igfs.IgfsMode.PROXY;

/**
 * Tests for node validation logic in {@link IgfsProcessor}.
 * <p>
 * Tests starting with "testLocal" are checking
 * {@link IgfsProcessor#validateLocalIgfsConfigurations(org.apache.ignite.configuration.FileSystemConfiguration[])}.
 * <p>
 * Tests starting with "testRemote" are checking {@link IgfsProcessor#checkIgfsOnRemoteNode(org.apache.ignite.cluster.ClusterNode)}.
 */
public class IgfsProcessorValidationSelfTest extends IgfsCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Grid #1 config. */
    private IgniteConfiguration g1Cfg;

    /** Data cache 1 name. */
    private static final String dataCache1Name = "dataCache1";

    /** Data cache 2 name. */
    private static final String dataCache2Name = "dataCache2";

    /** Meta cache 1 name. */
    private static final String metaCache1Name = "metaCache1";

    /** Meta cache 2 name. */
    private static final String metaCache2Name = "metaCache2";

    /** First IGFS config in grid #1. */
    private FileSystemConfiguration g1IgfsCfg1 = new FileSystemConfiguration();

    /** Second IGFS config in grid#1. */
    private FileSystemConfiguration g1IgfsCfg2 = new FileSystemConfiguration();

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        g1Cfg = getConfiguration("g1");
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        g1IgfsCfg1.setName("g1IgfsCfg1");
        g1IgfsCfg1.setDataCacheName(dataCache1Name);
        g1IgfsCfg1.setMetaCacheName(metaCache1Name);

        g1IgfsCfg2.setName("g1IgfsCfg2");
        g1IgfsCfg2.setDataCacheName(dataCache2Name);
        g1IgfsCfg2.setMetaCacheName(metaCache2Name);

        cfg.setFileSystemConfiguration(g1IgfsCfg1, g1IgfsCfg2);

        cfg.setLocalHost("127.0.0.1");

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Returns a new array that contains the concatenated contents of two arrays.
     *
     * @param first the first array of elements to concatenate.
     * @param second the second array of elements to concatenate.
     * @param cls
     * @return Concatenated array.
     */
    private <T> T[] concat(T[] first, T[] second, Class<?> cls) {
        Collection<T> res = new ArrayList<>();

        res.addAll(Arrays.asList(first));
        res.addAll(Arrays.asList(second));

        return res.toArray((T[]) Array.newInstance(cls, res.size()));
    }


    /**
     * @throws Exception If failed.
     */
    public void testLocalIfNoCacheIsConfigured() throws Exception {
        checkGridStartFails(g1Cfg, "Data cache is not configured locally for IGFS", true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testLocalIfNoDataCacheIsConfigured() throws Exception {
        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setName("someName");

        g1Cfg.setCacheConfiguration(cc);

        checkGridStartFails(g1Cfg, "Data cache is not configured locally for IGFS", true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testLocalIfNoMetadataCacheIsConfigured() throws Exception {
        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setName(dataCache1Name);

        g1Cfg.setCacheConfiguration(cc);

        checkGridStartFails(g1Cfg, "Metadata cache is not configured locally for IGFS", true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testLocalIfAffinityMapperIsWrongClass() throws Exception {
        g1Cfg.setCacheConfiguration(concat(dataCaches(1024), metaCaches(), CacheConfiguration.class));

        for (CacheConfiguration cc : g1Cfg.getCacheConfiguration())
            cc.setAffinityMapper(new GridCacheDefaultAffinityKeyMapper());

        checkGridStartFails(g1Cfg, "Invalid IGFS data cache configuration (key affinity mapper class should be", true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testLocalIfIgfsConfigsHaveDifferentNames() throws Exception {
        g1Cfg.setCacheConfiguration(concat(dataCaches(1024), metaCaches(), CacheConfiguration.class));

        String igfsCfgName = "igfs-cfg";

        g1IgfsCfg1.setName(igfsCfgName);
        g1IgfsCfg2.setName(igfsCfgName);

        checkGridStartFails(g1Cfg, "Duplicate IGFS name found (check configuration and assign unique name", true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testLocalIfQueryIndexingEnabledForDataCache() throws Exception {
        CacheConfiguration[] dataCaches = dataCaches(1024);

        dataCaches[0].setIndexedTypes(Integer.class, String.class);

        g1Cfg.setCacheConfiguration(concat(dataCaches, metaCaches(), CacheConfiguration.class));

        checkGridStartFails(g1Cfg, "IGFS data cache cannot start with enabled query indexing", true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testLocalIfQueryIndexingEnabledForMetaCache() throws Exception {
        CacheConfiguration[] metaCaches = metaCaches();

        metaCaches[0].setIndexedTypes(Integer.class, String.class);

        g1Cfg.setCacheConfiguration(concat(dataCaches(1024), metaCaches, CacheConfiguration.class));

        checkGridStartFails(g1Cfg, "IGFS metadata cache cannot start with enabled query indexing", true);
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("NullableProblems")
    public void testLocalNullIgfsNameIsSupported() throws Exception {
        g1Cfg.setCacheConfiguration(concat(dataCaches(1024), metaCaches(), CacheConfiguration.class));

        g1IgfsCfg1.setName(null);

        assertFalse(G.start(g1Cfg).cluster().nodes().isEmpty());
    }

    /**
     * @throws Exception If failed.
     */
    public void testLocalIfOffheapIsDisabledAndMaxSpaceSizeIsGreater() throws Exception {
        g1Cfg.setCacheConfiguration(concat(dataCaches(1024), metaCaches(), CacheConfiguration.class));

        g1IgfsCfg2.setMaxSpaceSize(999999999999999999L);

        checkGridStartFails(g1Cfg, "Maximum IGFS space size cannot be greater that size of available heap", true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testLocalIfOffheapIsEnabledAndMaxSpaceSizeIsGreater() throws Exception {
        g1Cfg.setCacheConfiguration(concat(dataCaches(1024), metaCaches(), CacheConfiguration.class));

        for (CacheConfiguration cc : g1Cfg.getCacheConfiguration())
            cc.setOffHeapMaxMemory(1000000);

        g1IgfsCfg2.setMaxSpaceSize(999999999999999999L);

        checkGridStartFails(g1Cfg,
            "Maximum IGFS space size cannot be greater than size of available heap memory and offheap storage", true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testLocalIfNonPrimaryModeAndHadoopFileSystemUriIsNull() throws Exception {
        g1Cfg.setCacheConfiguration(concat(dataCaches(1024), metaCaches(), CacheConfiguration.class));

        g1IgfsCfg2.setDefaultMode(PROXY);

        checkGridStartFails(g1Cfg, "secondaryFileSystem cannot be null when mode is not PRIMARY", true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoteIfDataBlockSizeDiffers() throws Exception {
        IgniteConfiguration g2Cfg = getConfiguration("g2");

        g1Cfg.setCacheConfiguration(concat(dataCaches(1024), metaCaches(), CacheConfiguration.class));
        g2Cfg.setCacheConfiguration(concat(dataCaches(1024), metaCaches(), CacheConfiguration.class));

        FileSystemConfiguration g2IgfsCfg1 = new FileSystemConfiguration(g1IgfsCfg1);

        g2IgfsCfg1.setBlockSize(g2IgfsCfg1.getBlockSize() + 100);

        g2Cfg.setFileSystemConfiguration(g2IgfsCfg1, g1IgfsCfg2);

        G.start(g1Cfg);

        checkGridStartFails(g2Cfg, "Data block size should be the same on all nodes in grid for IGFS", false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoteIfAffinityMapperGroupSizeDiffers() throws Exception {
        IgniteConfiguration g2Cfg = getConfiguration("g2");

        g1Cfg.setCacheConfiguration(concat(dataCaches(1024), metaCaches(), CacheConfiguration.class));
        g2Cfg.setCacheConfiguration(concat(dataCaches(4021), metaCaches(), CacheConfiguration.class));

        G.start(g1Cfg);

        checkGridStartFails(g2Cfg, "Affinity mapper group size should be the same on all nodes in grid for IGFS",
            false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoteIfMetaCacheNameDiffers() throws Exception {
        IgniteConfiguration g2Cfg = getConfiguration("g2");

        FileSystemConfiguration g2IgfsCfg1 = new FileSystemConfiguration(g1IgfsCfg1);
        FileSystemConfiguration g2IgfsCfg2 = new FileSystemConfiguration(g1IgfsCfg2);

        g2IgfsCfg1.setMetaCacheName("g2MetaCache1");
        g2IgfsCfg2.setMetaCacheName("g2MetaCache2");

        g1Cfg.setCacheConfiguration(concat(dataCaches(1024), metaCaches(), CacheConfiguration.class));
        g2Cfg.setCacheConfiguration(concat(dataCaches(1024), metaCaches("g2MetaCache1", "g2MetaCache2"),
             CacheConfiguration.class));

        g2Cfg.setFileSystemConfiguration(g2IgfsCfg1, g2IgfsCfg2);

        G.start(g1Cfg);

        checkGridStartFails(g2Cfg, "Meta cache name should be the same on all nodes in grid for IGFS", false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoteIfMetaCacheNameEquals() throws Exception {
        IgniteConfiguration g2Cfg = getConfiguration("g2");

        FileSystemConfiguration g2IgfsCfg1 = new FileSystemConfiguration(g1IgfsCfg1);
        FileSystemConfiguration g2IgfsCfg2 = new FileSystemConfiguration(g1IgfsCfg2);

        g2IgfsCfg1.setName("g2IgfsCfg1");
        g2IgfsCfg2.setName("g2IgfsCfg2");

        g2IgfsCfg1.setDataCacheName("g2DataCache1");
        g2IgfsCfg2.setDataCacheName("g2DataCache2");

        g1Cfg.setCacheConfiguration(concat(dataCaches(1024), metaCaches(), CacheConfiguration.class));
        g2Cfg.setCacheConfiguration(concat(dataCaches(1024, "g2DataCache1", "g2DataCache2"), metaCaches(),
             CacheConfiguration.class));

        g2Cfg.setFileSystemConfiguration(g2IgfsCfg1, g2IgfsCfg2);

        G.start(g1Cfg);

        checkGridStartFails(g2Cfg, "Meta cache names should be different for different IGFS instances", false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoteIfDataCacheNameDiffers() throws Exception {
        IgniteConfiguration g2Cfg = getConfiguration("g2");

        FileSystemConfiguration g2IgfsCfg1 = new FileSystemConfiguration(g1IgfsCfg1);
        FileSystemConfiguration g2IgfsCfg2 = new FileSystemConfiguration(g1IgfsCfg2);

        g2IgfsCfg1.setDataCacheName("g2DataCache1");
        g2IgfsCfg2.setDataCacheName("g2DataCache2");

        g1Cfg.setCacheConfiguration(concat(dataCaches(1024), metaCaches(), CacheConfiguration.class));
        g2Cfg.setCacheConfiguration(concat(dataCaches(1024, "g2DataCache1", "g2DataCache2"), metaCaches(),
             CacheConfiguration.class));

        g2Cfg.setFileSystemConfiguration(g2IgfsCfg1, g2IgfsCfg2);

        G.start(g1Cfg);

        checkGridStartFails(g2Cfg, "Data cache name should be the same on all nodes in grid for IGFS", false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoteIfDataCacheNameEquals() throws Exception {
        IgniteConfiguration g2Cfg = getConfiguration("g2");

        FileSystemConfiguration g2IgfsCfg1 = new FileSystemConfiguration(g1IgfsCfg1);
        FileSystemConfiguration g2IgfsCfg2 = new FileSystemConfiguration(g1IgfsCfg2);

        g2IgfsCfg1.setName("g2IgfsCfg1");
        g2IgfsCfg2.setName("g2IgfsCfg2");

        g2IgfsCfg1.setMetaCacheName("g2MetaCache1");
        g2IgfsCfg2.setMetaCacheName("g2MetaCache2");

        g1Cfg.setCacheConfiguration(concat(dataCaches(1024), metaCaches(), CacheConfiguration.class));
        g2Cfg.setCacheConfiguration(concat(dataCaches(1024), metaCaches("g2MetaCache1", "g2MetaCache2"),
             CacheConfiguration.class));

        g2Cfg.setFileSystemConfiguration(g2IgfsCfg1, g2IgfsCfg2);

        G.start(g1Cfg);

        checkGridStartFails(g2Cfg, "Data cache names should be different for different IGFS instances", false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoteIfDefaultModeDiffers() throws Exception {
        IgniteConfiguration g2Cfg = getConfiguration("g2");

        FileSystemConfiguration g2IgfsCfg1 = new FileSystemConfiguration(g1IgfsCfg1);
        FileSystemConfiguration g2IgfsCfg2 = new FileSystemConfiguration(g1IgfsCfg2);

        g1IgfsCfg1.setDefaultMode(DUAL_ASYNC);
        g1IgfsCfg2.setDefaultMode(DUAL_ASYNC);

        g2IgfsCfg1.setDefaultMode(DUAL_SYNC);
        g2IgfsCfg2.setDefaultMode(DUAL_SYNC);

        g1Cfg.setCacheConfiguration(concat(dataCaches(1024), metaCaches(), CacheConfiguration.class));
        g2Cfg.setCacheConfiguration(concat(dataCaches(1024), metaCaches(), CacheConfiguration.class));

        g2Cfg.setFileSystemConfiguration(g2IgfsCfg1, g2IgfsCfg2);

        G.start(g1Cfg);

        checkGridStartFails(g2Cfg, "Default mode should be the same on all nodes in grid for IGFS", false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoteIfPathModeDiffers() throws Exception {
        IgniteConfiguration g2Cfg = getConfiguration("g2");

        FileSystemConfiguration g2IgfsCfg1 = new FileSystemConfiguration(g1IgfsCfg1);
        FileSystemConfiguration g2IgfsCfg2 = new FileSystemConfiguration(g1IgfsCfg2);

        g2IgfsCfg1.setPathModes(Collections.singletonMap("/somePath", DUAL_SYNC));
        g2IgfsCfg2.setPathModes(Collections.singletonMap("/somePath", DUAL_SYNC));

        g1Cfg.setCacheConfiguration(concat(dataCaches(1024), metaCaches(), CacheConfiguration.class));
        g2Cfg.setCacheConfiguration(concat(dataCaches(1024), metaCaches(), CacheConfiguration.class));

        g2Cfg.setFileSystemConfiguration(g2IgfsCfg1, g2IgfsCfg2);

        G.start(g1Cfg);

        checkGridStartFails(g2Cfg, "Path modes should be the same on all nodes in grid for IGFS", false);
    }

    /**
     * Checks that the given grid configuration will lead to {@link IgniteCheckedException} upon grid startup.
     *
     * @param cfg Grid configuration to check.
     * @param excMsgSnippet Root cause (assertion) exception message snippet.
     * @param testLoc {@code True} if checking is done for "testLocal" tests.
     */
    private void checkGridStartFails(IgniteConfiguration cfg, CharSequence excMsgSnippet, boolean testLoc) {
        assertNotNull(cfg);
        assertNotNull(excMsgSnippet);

        try {
            G.start(cfg);

            fail("No exception has been thrown.");
        }
        catch (IgniteException e) {
            if (testLoc) {
                if ("Failed to start processor: GridProcessorAdapter []".equals(e.getMessage()) &&
                    (e.getCause().getMessage().contains(excMsgSnippet) ||
                     e.getCause().getCause().getMessage().contains(excMsgSnippet)))
                    return; // Expected exception.
            }
            else if (e.getMessage().contains(excMsgSnippet))
                return; // Expected exception.

            error("Caught unexpected exception.", e);

            fail();
        }
    }

    /**
     * @param grpSize Group size to use in {@link org.apache.ignite.igfs.IgfsGroupDataBlocksKeyMapper}.
     * @param cacheNames 2 Optional caches names.
     * @return 2 preconfigured data caches.
     */
    private CacheConfiguration[] dataCaches(int grpSize, String... cacheNames) {
        assertTrue(F.isEmpty(cacheNames) || cacheNames.length == 2);

        if (F.isEmpty(cacheNames))
            cacheNames = new String[] {dataCache1Name, dataCache2Name};

        CacheConfiguration[] res = new CacheConfiguration[cacheNames.length];

        for (int i = 0; i < cacheNames.length; i++) {
            CacheConfiguration dataCache = defaultCacheConfiguration();

            dataCache.setName(cacheNames[i]);
            dataCache.setAffinityMapper(new IgfsGroupDataBlocksKeyMapper(grpSize));
            dataCache.setAtomicityMode(TRANSACTIONAL);

            res[i] = dataCache;
        }

        return res;
    }

    /**
     * @param cacheNames 2 Optional caches names.
     * @return 2 preconfigured meta caches.
     */
    private CacheConfiguration[] metaCaches(String... cacheNames) {
        assertTrue(F.isEmpty(cacheNames) || cacheNames.length == 2);

        if (F.isEmpty(cacheNames))
            cacheNames = new String[] {metaCache1Name, metaCache2Name};

        CacheConfiguration[] res = new CacheConfiguration[cacheNames.length];

        for (int i = 0; i < cacheNames.length; i++) {
            CacheConfiguration metaCache = defaultCacheConfiguration();

            metaCache.setName(cacheNames[i]);
            metaCache.setAtomicityMode(TRANSACTIONAL);

            res[i] = metaCache;
        }

        return res;
    }
}