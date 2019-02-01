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
import org.apache.ignite.igfs.IgfsIpcEndpointConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheDefaultAffinityKeyMapper;
import org.apache.ignite.internal.util.typedef.G;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.igfs.IgfsMode.DUAL_ASYNC;
import static org.apache.ignite.igfs.IgfsMode.DUAL_SYNC;
import static org.apache.ignite.igfs.IgfsMode.PROXY;

/**
 * Tests for node validation logic in {@link IgfsProcessor}.
 * <p>
 * Tests starting with "testLocal" are checking
 * {@link IgfsUtils#validateLocalIgfsConfigurations(org.apache.ignite.configuration.IgniteConfiguration)}.
 * <p>
 * Tests starting with "testRemote" are checking {@link IgfsProcessor#checkIgfsOnRemoteNode(org.apache.ignite.cluster.ClusterNode)}.
 */
public class IgfsProcessorValidationSelfTest extends IgfsCommonAbstractTest {
    /** Grid #1 config. */
    private IgniteConfiguration g1Cfg;

    /** First IGFS config in grid #1. */
    private FileSystemConfiguration g1IgfsCfg1 = new FileSystemConfiguration();

    /** Second IGFS config in grid#1. */
    private FileSystemConfiguration g1IgfsCfg2 = new FileSystemConfiguration();

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        g1Cfg = getConfiguration("g1");
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        g1IgfsCfg1.setName("g1IgfsCfg1");

        g1IgfsCfg2.setName("g1IgfsCfg2");

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
     * @param cls Class of elements.
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
    @Test
    public void testLocalIfAffinityMapperIsWrongClass() throws Exception {

        for (FileSystemConfiguration igfsCfg : g1Cfg.getFileSystemConfiguration()) {
            igfsCfg.setDataCacheConfiguration(dataCache(1024));
            igfsCfg.setMetaCacheConfiguration(metaCache());

            igfsCfg.getMetaCacheConfiguration().setAffinityMapper(new GridCacheDefaultAffinityKeyMapper());
            igfsCfg.getDataCacheConfiguration().setAffinityMapper(new GridCacheDefaultAffinityKeyMapper());
        }

        checkGridStartFails(g1Cfg, "Invalid IGFS data cache configuration (key affinity mapper class should be", true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLocalIfIgfsConfigsHaveDuplicatedNames() throws Exception {
        String igfsCfgName = "igfs-cfg";

        g1IgfsCfg1.setName(igfsCfgName);
        g1IgfsCfg2.setName(igfsCfgName);

        checkGridStartFails(g1Cfg, "Duplicate IGFS name found (check configuration and assign unique name", true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLocalIfQueryIndexingEnabledForDataCache() throws Exception {
        g1IgfsCfg1.setDataCacheConfiguration(dataCache(1024));
        g1IgfsCfg1.getDataCacheConfiguration().setIndexedTypes(Integer.class, String.class);

        checkGridStartFails(g1Cfg, "IGFS data cache cannot start with enabled query indexing", true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLocalIfQueryIndexingEnabledForMetaCache() throws Exception {
        g1IgfsCfg1.setMetaCacheConfiguration(metaCache());

        g1IgfsCfg1.getMetaCacheConfiguration().setIndexedTypes(Integer.class, String.class);

        checkGridStartFails(g1Cfg, "IGFS metadata cache cannot start with enabled query indexing", true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLocalNullIgfsNameIsNotSupported() throws Exception {
        try {
            g1IgfsCfg1.setName(null);

            fail("IGFS name cannot be null");
        }
        catch (IllegalArgumentException e) {
            // No-op.
        }

        ArrayList<FileSystemConfiguration> fsCfgs = new ArrayList<>(Arrays.asList(g1Cfg.getFileSystemConfiguration()));

        fsCfgs.add(new FileSystemConfiguration()); // IGFS doesn't have default name (name == null).

        g1Cfg.setFileSystemConfiguration(fsCfgs.toArray(new FileSystemConfiguration[fsCfgs.size()]));

        checkGridStartFails(g1Cfg, "IGFS name cannot be null", true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLocalIfNonPrimaryModeAndHadoopFileSystemUriIsNull() throws Exception {
        g1IgfsCfg2.setDefaultMode(PROXY);

        checkGridStartFails(g1Cfg, "secondaryFileSystem cannot be null when mode is not PRIMARY", true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRemoteIfDataBlockSizeDiffers() throws Exception {
        IgniteConfiguration g2Cfg = getConfiguration("g2");

        FileSystemConfiguration g2IgfsCfg1 = new FileSystemConfiguration(g1IgfsCfg1);

        g2IgfsCfg1.setBlockSize(g2IgfsCfg1.getBlockSize() + 100);

        g2Cfg.setFileSystemConfiguration(g2IgfsCfg1, g1IgfsCfg2);

        G.start(g1Cfg);

        checkGridStartFails(g2Cfg, "Data block size should be the same on all nodes in grid for IGFS", false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRemoteIfAffinityMapperGroupSizeDiffers() throws Exception {
        IgniteConfiguration g2Cfg = getConfiguration("g2");

        G.start(g1Cfg);

        for (FileSystemConfiguration igfsCfg : g2Cfg.getFileSystemConfiguration())
            igfsCfg.setDataCacheConfiguration(dataCache(1000));

        checkGridStartFails(g2Cfg, "Affinity mapper group size should be the same on all nodes in grid for IGFS",
            false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRemoteIfDefaultModeDiffers() throws Exception {
        IgniteConfiguration g2Cfg = getConfiguration("g2");

        FileSystemConfiguration g2IgfsCfg1 = new FileSystemConfiguration(g1IgfsCfg1);
        FileSystemConfiguration g2IgfsCfg2 = new FileSystemConfiguration(g1IgfsCfg2);

        g1IgfsCfg1.setDefaultMode(DUAL_ASYNC);
        g1IgfsCfg2.setDefaultMode(DUAL_ASYNC);

        g2IgfsCfg1.setDefaultMode(DUAL_SYNC);
        g2IgfsCfg2.setDefaultMode(DUAL_SYNC);

        g2Cfg.setFileSystemConfiguration(g2IgfsCfg1, g2IgfsCfg2);

        G.start(g1Cfg);

        checkGridStartFails(g2Cfg, "Default mode should be the same on all nodes in grid for IGFS", false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRemoteIfPathModeDiffers() throws Exception {
        IgniteConfiguration g2Cfg = getConfiguration("g2");

        FileSystemConfiguration g2IgfsCfg1 = new FileSystemConfiguration(g1IgfsCfg1);
        FileSystemConfiguration g2IgfsCfg2 = new FileSystemConfiguration(g1IgfsCfg2);

        g2IgfsCfg1.setPathModes(Collections.singletonMap("/somePath", DUAL_SYNC));
        g2IgfsCfg2.setPathModes(Collections.singletonMap("/somePath", DUAL_SYNC));

        g2Cfg.setFileSystemConfiguration(g2IgfsCfg1, g2IgfsCfg2);

        G.start(g1Cfg);

        checkGridStartFails(g2Cfg, "Path modes should be the same on all nodes in grid for IGFS", false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testZeroEndpointTcpPort() throws Exception {
        checkInvalidPort(0);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNegativeEndpointTcpPort() throws Exception {
        checkInvalidPort(-1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTooBigEndpointTcpPort() throws Exception {
        checkInvalidPort(65536);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPreConfiguredCache() throws Exception {
        FileSystemConfiguration igfsCfg1 = new FileSystemConfiguration(g1IgfsCfg1);
        igfsCfg1.setName("igfs");

        g1Cfg.setFileSystemConfiguration(igfsCfg1);

        CacheConfiguration ccfgData = dataCache(1024);
        ccfgData.setRebalanceTimeout(10001);

        CacheConfiguration ccfgMeta = metaCache();
        ccfgMeta.setRebalanceTimeout(10002);

        igfsCfg1.setDataCacheConfiguration(ccfgData);
        igfsCfg1.setMetaCacheConfiguration(ccfgMeta);

        IgniteEx g = (IgniteEx)G.start(g1Cfg);

        assertEquals(10001,
            g.cachex(g.igfsx("igfs").configuration().getDataCacheConfiguration().getName())
                .configuration().getRebalanceTimeout());
        assertEquals(10002,
            g.cachex(g.igfsx("igfs").configuration().getMetaCacheConfiguration().getName())
                .configuration().getRebalanceTimeout());
    }

    /**
     * Check invalid port handling.
     *
     * @param port Port.
     * @throws Exception If failed.
     */
    private void checkInvalidPort(int port) throws Exception {
        final String failMsg = "IGFS endpoint TCP port is out of range";

        final String igfsCfgName = "igfs-cfg";
        final IgfsIpcEndpointConfiguration igfsEndpointCfg = new IgfsIpcEndpointConfiguration();
        igfsEndpointCfg.setPort(port);
        g1IgfsCfg1.setName(igfsCfgName);
        g1IgfsCfg1.setIpcEndpointConfiguration(igfsEndpointCfg);

        checkGridStartFails(g1Cfg, failMsg, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInvalidEndpointThreadCount() throws Exception {
        final String failMsg = "IGFS endpoint thread count must be positive";

        final String igfsCfgName = "igfs-cfg";
        final IgfsIpcEndpointConfiguration igfsEndpointCfg = new IgfsIpcEndpointConfiguration();
        igfsEndpointCfg.setThreadCount(0);
        g1IgfsCfg1.setName(igfsCfgName);
        g1IgfsCfg1.setIpcEndpointConfiguration(igfsEndpointCfg);

        checkGridStartFails(g1Cfg, failMsg, true);
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
                if (e.getMessage().contains(excMsgSnippet)
                    || ("Failed to start processor: GridProcessorAdapter []".equals(e.getMessage()) &&
                    (e.getCause().getMessage().contains(excMsgSnippet) ||
                     e.getCause().getCause().getMessage().contains(excMsgSnippet))))
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
     * @return 2 preconfigured data cache.
     */
    private CacheConfiguration dataCache(int grpSize) {

        CacheConfiguration dataCache = defaultCacheConfiguration();

        dataCache.setAffinityMapper(new IgfsGroupDataBlocksKeyMapper(grpSize));
        dataCache.setAtomicityMode(TRANSACTIONAL);

        return dataCache;
    }

    /**
     * @return preconfigured meta cache.
     */
    private CacheConfiguration metaCache() {
        CacheConfiguration metaCache = defaultCacheConfiguration();

        metaCache.setAtomicityMode(TRANSACTIONAL);

        return metaCache;
    }
}
