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

package org.apache.ignite.internal.processors.fs;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.ignitefs.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;

import java.lang.reflect.*;
import java.util.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.ignitefs.IgniteFsMode.*;

/**
 * Tests for node validation logic in {@link IgniteFsProcessor}.
 * <p>
 * Tests starting with "testLocal" are checking
 * {@link IgniteFsProcessor#validateLocalGgfsConfigurations(org.apache.ignite.configuration.IgniteFsConfiguration[])}.
 * <p>
 * Tests starting with "testRemote" are checking {@link IgniteFsProcessor#checkGgfsOnRemoteNode(org.apache.ignite.cluster.ClusterNode)}.
 */
public class GridGgfsProcessorValidationSelfTest extends GridGgfsCommonAbstractTest {
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

    /** First GGFS config in grid #1. */
    private IgniteFsConfiguration g1GgfsCfg1 = new IgniteFsConfiguration();

    /** Second GGFS config in grid#1. */
    private IgniteFsConfiguration g1GgfsCfg2 = new IgniteFsConfiguration();

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

        g1GgfsCfg1.setName("g1GgfsCfg1");
        g1GgfsCfg1.setDataCacheName(dataCache1Name);
        g1GgfsCfg1.setMetaCacheName(metaCache1Name);

        g1GgfsCfg2.setName("g1GgfsCfg2");
        g1GgfsCfg2.setDataCacheName(dataCache2Name);
        g1GgfsCfg2.setMetaCacheName(metaCache2Name);

        cfg.setGgfsConfiguration(g1GgfsCfg1, g1GgfsCfg2);

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
        checkGridStartFails(g1Cfg, "Data cache is not configured locally for GGFS", true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testLocalIfNoDataCacheIsConfigured() throws Exception {
        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setQueryIndexEnabled(false);
        cc.setName("someName");

        g1Cfg.setCacheConfiguration(cc);

        checkGridStartFails(g1Cfg, "Data cache is not configured locally for GGFS", true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testLocalIfNoMetadataCacheIsConfigured() throws Exception {
        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setQueryIndexEnabled(false);
        cc.setName(dataCache1Name);

        g1Cfg.setCacheConfiguration(cc);

        checkGridStartFails(g1Cfg, "Metadata cache is not configured locally for GGFS", true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testLocalIfAffinityMapperIsWrongClass() throws Exception {
        g1Cfg.setCacheConfiguration(concat(dataCaches(1024), metaCaches(), CacheConfiguration.class));

        for (CacheConfiguration cc : g1Cfg.getCacheConfiguration())
            cc.setAffinityMapper(new GridCacheDefaultAffinityKeyMapper());

        checkGridStartFails(g1Cfg, "Invalid GGFS data cache configuration (key affinity mapper class should be", true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testLocalIfGgfsConfigsHaveDifferentNames() throws Exception {
        g1Cfg.setCacheConfiguration(concat(dataCaches(1024), metaCaches(), CacheConfiguration.class));

        String ggfsCfgName = "ggfs-cfg";

        g1GgfsCfg1.setName(ggfsCfgName);
        g1GgfsCfg2.setName(ggfsCfgName);

        checkGridStartFails(g1Cfg, "Duplicate GGFS name found (check configuration and assign unique name", true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testLocalIfQueryIndexingEnabledForDataCache() throws Exception {
        CacheConfiguration[] dataCaches = dataCaches(1024);

        dataCaches[0].setQueryIndexEnabled(true);

        g1Cfg.setCacheConfiguration(concat(dataCaches, metaCaches(), CacheConfiguration.class));

        checkGridStartFails(g1Cfg, "GGFS data cache cannot start with enabled query indexing", true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testLocalIfQueryIndexingEnabledForMetaCache() throws Exception {
        CacheConfiguration[] metaCaches = metaCaches();

        metaCaches[0].setQueryIndexEnabled(true);

        g1Cfg.setCacheConfiguration(concat(dataCaches(1024), metaCaches, CacheConfiguration.class));

        checkGridStartFails(g1Cfg, "GGFS metadata cache cannot start with enabled query indexing", true);
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("NullableProblems")
    public void testLocalNullGgfsNameIsSupported() throws Exception {
        g1Cfg.setCacheConfiguration(concat(dataCaches(1024), metaCaches(), CacheConfiguration.class));

        g1GgfsCfg1.setName(null);

        assertFalse(G.start(g1Cfg).cluster().nodes().isEmpty());
    }

    /**
     * @throws Exception If failed.
     */
    public void testLocalIfOffheapIsDisabledAndMaxSpaceSizeIsGreater() throws Exception {
        g1Cfg.setCacheConfiguration(concat(dataCaches(1024), metaCaches(), CacheConfiguration.class));

        g1GgfsCfg2.setMaxSpaceSize(999999999999999999L);

        checkGridStartFails(g1Cfg, "Maximum GGFS space size cannot be greater that size of available heap", true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testLocalIfOffheapIsEnabledAndMaxSpaceSizeIsGreater() throws Exception {
        g1Cfg.setCacheConfiguration(concat(dataCaches(1024), metaCaches(), CacheConfiguration.class));

        for (CacheConfiguration cc : g1Cfg.getCacheConfiguration())
            cc.setOffHeapMaxMemory(1000000);

        g1GgfsCfg2.setMaxSpaceSize(999999999999999999L);

        checkGridStartFails(g1Cfg,
            "Maximum GGFS space size cannot be greater than size of available heap memory and offheap storage", true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testLocalIfBackupsEnabled() throws Exception {
        g1Cfg.setCacheConfiguration(concat(dataCaches(1024), metaCaches(), CacheConfiguration.class));

        for (CacheConfiguration cc : g1Cfg.getCacheConfiguration()) {
            cc.setCacheMode(PARTITIONED);
            cc.setBackups(1);
        }

        checkGridStartFails(g1Cfg, "GGFS data cache cannot be used with backups", true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testLocalIfNonPrimaryModeAndHadoopFileSystemUriIsNull() throws Exception {
        g1Cfg.setCacheConfiguration(concat(dataCaches(1024), metaCaches(), CacheConfiguration.class));

        g1GgfsCfg2.setDefaultMode(PROXY);

        checkGridStartFails(g1Cfg, "secondaryFileSystem cannot be null when mode is SECONDARY", true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoteIfDataBlockSizeDiffers() throws Exception {
        IgniteConfiguration g2Cfg = getConfiguration("g2");

        g1Cfg.setCacheConfiguration(concat(dataCaches(1024), metaCaches(), CacheConfiguration.class));
        g2Cfg.setCacheConfiguration(concat(dataCaches(1024), metaCaches(), CacheConfiguration.class));

        IgniteFsConfiguration g2GgfsCfg1 = new IgniteFsConfiguration(g1GgfsCfg1);

        g2GgfsCfg1.setBlockSize(g2GgfsCfg1.getBlockSize() + 100);

        g2Cfg.setGgfsConfiguration(g2GgfsCfg1, g1GgfsCfg2);

        G.start(g1Cfg);

        checkGridStartFails(g2Cfg, "Data block size should be the same on all nodes in grid for GGFS", false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoteIfAffinityMapperGroupSizeDiffers() throws Exception {
        IgniteConfiguration g2Cfg = getConfiguration("g2");

        g1Cfg.setCacheConfiguration(concat(dataCaches(1024), metaCaches(), CacheConfiguration.class));
        g2Cfg.setCacheConfiguration(concat(dataCaches(4021), metaCaches(), CacheConfiguration.class));

        G.start(g1Cfg);

        checkGridStartFails(g2Cfg, "Affinity mapper group size should be the same on all nodes in grid for GGFS",
            false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoteIfMetaCacheNameDiffers() throws Exception {
        IgniteConfiguration g2Cfg = getConfiguration("g2");

        IgniteFsConfiguration g2GgfsCfg1 = new IgniteFsConfiguration(g1GgfsCfg1);
        IgniteFsConfiguration g2GgfsCfg2 = new IgniteFsConfiguration(g1GgfsCfg2);

        g2GgfsCfg1.setMetaCacheName("g2MetaCache1");
        g2GgfsCfg2.setMetaCacheName("g2MetaCache2");

        g1Cfg.setCacheConfiguration(concat(dataCaches(1024), metaCaches(), CacheConfiguration.class));
        g2Cfg.setCacheConfiguration(concat(dataCaches(1024), metaCaches("g2MetaCache1", "g2MetaCache2"),
             CacheConfiguration.class));

        g2Cfg.setGgfsConfiguration(g2GgfsCfg1, g2GgfsCfg2);

        G.start(g1Cfg);

        checkGridStartFails(g2Cfg, "Meta cache name should be the same on all nodes in grid for GGFS", false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoteIfMetaCacheNameEquals() throws Exception {
        IgniteConfiguration g2Cfg = getConfiguration("g2");

        IgniteFsConfiguration g2GgfsCfg1 = new IgniteFsConfiguration(g1GgfsCfg1);
        IgniteFsConfiguration g2GgfsCfg2 = new IgniteFsConfiguration(g1GgfsCfg2);

        g2GgfsCfg1.setName("g2GgfsCfg1");
        g2GgfsCfg2.setName("g2GgfsCfg2");

        g2GgfsCfg1.setDataCacheName("g2DataCache1");
        g2GgfsCfg2.setDataCacheName("g2DataCache2");

        g1Cfg.setCacheConfiguration(concat(dataCaches(1024), metaCaches(), CacheConfiguration.class));
        g2Cfg.setCacheConfiguration(concat(dataCaches(1024, "g2DataCache1", "g2DataCache2"), metaCaches(),
             CacheConfiguration.class));

        g2Cfg.setGgfsConfiguration(g2GgfsCfg1, g2GgfsCfg2);

        G.start(g1Cfg);

        checkGridStartFails(g2Cfg, "Meta cache names should be different for different GGFS instances", false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoteIfDataCacheNameDiffers() throws Exception {
        IgniteConfiguration g2Cfg = getConfiguration("g2");

        IgniteFsConfiguration g2GgfsCfg1 = new IgniteFsConfiguration(g1GgfsCfg1);
        IgniteFsConfiguration g2GgfsCfg2 = new IgniteFsConfiguration(g1GgfsCfg2);

        g2GgfsCfg1.setDataCacheName("g2DataCache1");
        g2GgfsCfg2.setDataCacheName("g2DataCache2");

        g1Cfg.setCacheConfiguration(concat(dataCaches(1024), metaCaches(), CacheConfiguration.class));
        g2Cfg.setCacheConfiguration(concat(dataCaches(1024, "g2DataCache1", "g2DataCache2"), metaCaches(),
             CacheConfiguration.class));

        g2Cfg.setGgfsConfiguration(g2GgfsCfg1, g2GgfsCfg2);

        G.start(g1Cfg);

        checkGridStartFails(g2Cfg, "Data cache name should be the same on all nodes in grid for GGFS", false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoteIfDataCacheNameEquals() throws Exception {
        IgniteConfiguration g2Cfg = getConfiguration("g2");

        IgniteFsConfiguration g2GgfsCfg1 = new IgniteFsConfiguration(g1GgfsCfg1);
        IgniteFsConfiguration g2GgfsCfg2 = new IgniteFsConfiguration(g1GgfsCfg2);

        g2GgfsCfg1.setName("g2GgfsCfg1");
        g2GgfsCfg2.setName("g2GgfsCfg2");

        g2GgfsCfg1.setMetaCacheName("g2MetaCache1");
        g2GgfsCfg2.setMetaCacheName("g2MetaCache2");

        g1Cfg.setCacheConfiguration(concat(dataCaches(1024), metaCaches(), CacheConfiguration.class));
        g2Cfg.setCacheConfiguration(concat(dataCaches(1024), metaCaches("g2MetaCache1", "g2MetaCache2"),
             CacheConfiguration.class));

        g2Cfg.setGgfsConfiguration(g2GgfsCfg1, g2GgfsCfg2);

        G.start(g1Cfg);

        checkGridStartFails(g2Cfg, "Data cache names should be different for different GGFS instances", false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoteIfDefaultModeDiffers() throws Exception {
        IgniteConfiguration g2Cfg = getConfiguration("g2");

        IgniteFsConfiguration g2GgfsCfg1 = new IgniteFsConfiguration(g1GgfsCfg1);
        IgniteFsConfiguration g2GgfsCfg2 = new IgniteFsConfiguration(g1GgfsCfg2);

        g1GgfsCfg1.setDefaultMode(DUAL_ASYNC);
        g1GgfsCfg2.setDefaultMode(DUAL_ASYNC);

        g2GgfsCfg1.setDefaultMode(DUAL_SYNC);
        g2GgfsCfg2.setDefaultMode(DUAL_SYNC);

        g1Cfg.setCacheConfiguration(concat(dataCaches(1024), metaCaches(), CacheConfiguration.class));
        g2Cfg.setCacheConfiguration(concat(dataCaches(1024), metaCaches(), CacheConfiguration.class));

        g2Cfg.setGgfsConfiguration(g2GgfsCfg1, g2GgfsCfg2);

        G.start(g1Cfg);

        checkGridStartFails(g2Cfg, "Default mode should be the same on all nodes in grid for GGFS", false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoteIfPathModeDiffers() throws Exception {
        IgniteConfiguration g2Cfg = getConfiguration("g2");

        IgniteFsConfiguration g2GgfsCfg1 = new IgniteFsConfiguration(g1GgfsCfg1);
        IgniteFsConfiguration g2GgfsCfg2 = new IgniteFsConfiguration(g1GgfsCfg2);

        g2GgfsCfg1.setPathModes(Collections.singletonMap("/somePath", DUAL_SYNC));
        g2GgfsCfg2.setPathModes(Collections.singletonMap("/somePath", DUAL_SYNC));

        g1Cfg.setCacheConfiguration(concat(dataCaches(1024), metaCaches(), CacheConfiguration.class));
        g2Cfg.setCacheConfiguration(concat(dataCaches(1024), metaCaches(), CacheConfiguration.class));

        g2Cfg.setGgfsConfiguration(g2GgfsCfg1, g2GgfsCfg2);

        G.start(g1Cfg);

        checkGridStartFails(g2Cfg, "Path modes should be the same on all nodes in grid for GGFS", false);
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
     * @param grpSize Group size to use in {@link org.apache.ignite.ignitefs.IgniteFsGroupDataBlocksKeyMapper}.
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
            dataCache.setAffinityMapper(new IgniteFsGroupDataBlocksKeyMapper(grpSize));
            dataCache.setAtomicityMode(TRANSACTIONAL);
            dataCache.setQueryIndexEnabled(false);

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
            metaCache.setQueryIndexEnabled(false);

            res[i] = metaCache;
        }

        return res;
    }
}
