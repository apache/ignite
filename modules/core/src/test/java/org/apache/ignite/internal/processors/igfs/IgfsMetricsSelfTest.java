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

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.igfs.IgfsGroupDataBlocksKeyMapper;
import org.apache.ignite.igfs.IgfsIpcEndpointConfiguration;
import org.apache.ignite.igfs.IgfsIpcEndpointType;
import org.apache.ignite.igfs.IgfsMode;
import org.apache.ignite.igfs.secondary.IgfsSecondaryFileSystem;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.igfs.IgfsMode.DUAL_SYNC;
import static org.apache.ignite.igfs.IgfsMode.PRIMARY;
import static org.apache.ignite.igfs.IgfsMode.PROXY;

/**
 * Test for IGFS metrics.
 */
public class IgfsMetricsSelfTest extends IgfsCommonAbstractTest {
    /** Primary IGFS name. */
    private static final String IGFS_PRIMARY = "igfs-primary";

    /** Primary IGFS name. */
    private static final String IGFS_SECONDARY = "igfs-secondary";

    /** Secondary file system REST endpoint configuration map. */
    private static final IgfsIpcEndpointConfiguration SECONDARY_REST_CFG;

    /** Test nodes count. */
    private static final int NODES_CNT = 3;

    /** IP finder for the grid with the primary file system. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Primary IGFS instances. */
    private static IgniteFileSystem[] igfsPrimary;

    /** Secondary IGFS instance. */
    private static IgfsImpl igfsSecondary;

    /** Secondary file system. */
    private static IgfsSecondaryFileSystem secFileSys;

    /** Primary file system block size. */
    public static final int PRIMARY_BLOCK_SIZE = 512;

    /** Secondary file system block size. */
    public static final int SECONDARY_BLOCK_SIZE = 512;

    static {
        SECONDARY_REST_CFG = new IgfsIpcEndpointConfiguration();

        SECONDARY_REST_CFG.setType(IgfsIpcEndpointType.TCP);
        SECONDARY_REST_CFG.setPort(11500);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startSecondary();
        startPrimary();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids(false);
    }

    /**
     * Start a grid with the primary file system.
     *
     * @throws Exception If failed.
     */
    private void startPrimary() throws Exception {
        igfsPrimary = new IgniteFileSystem[NODES_CNT];

        for (int i = 0; i < NODES_CNT; i++) {
            Ignite g = G.start(primaryConfiguration(i));

            igfsPrimary[i] = g.fileSystem(IGFS_PRIMARY);
        }
    }

    /**
     * Get configuration for a grid with the primary file system.
     *
     * @param idx Node index.
     * @return Configuration.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private IgniteConfiguration primaryConfiguration(int idx) throws Exception {
        FileSystemConfiguration igfsCfg = new FileSystemConfiguration();

        igfsCfg.setDataCacheName("dataCache");
        igfsCfg.setMetaCacheName("metaCache");
        igfsCfg.setName(IGFS_PRIMARY);
        igfsCfg.setBlockSize(PRIMARY_BLOCK_SIZE);
        igfsCfg.setDefaultMode(DUAL_SYNC);

        secFileSys = igfsSecondary.asSecondary();

        igfsCfg.setSecondaryFileSystem(secFileSys);

        Map<String, IgfsMode> pathModes = new HashMap<>();

        pathModes.put("/primary", PRIMARY);

        igfsCfg.setPathModes(pathModes);

        CacheConfiguration dataCacheCfg = defaultCacheConfiguration();

        dataCacheCfg.setName("dataCache");
        dataCacheCfg.setCacheMode(PARTITIONED);
        dataCacheCfg.setNearConfiguration(null);
        dataCacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        dataCacheCfg.setAffinityMapper(new IgfsGroupDataBlocksKeyMapper(128));
        dataCacheCfg.setBackups(0);
        dataCacheCfg.setAtomicityMode(TRANSACTIONAL);

        CacheConfiguration metaCacheCfg = defaultCacheConfiguration();

        metaCacheCfg.setName("metaCache");
        metaCacheCfg.setCacheMode(REPLICATED);
        metaCacheCfg.setNearConfiguration(null);
        metaCacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        metaCacheCfg.setAtomicityMode(TRANSACTIONAL);

        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setGridName("grid-" + idx);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);
        cfg.setCacheConfiguration(dataCacheCfg, metaCacheCfg);
        cfg.setFileSystemConfiguration(igfsCfg);

        cfg.setLocalHost("127.0.0.1");

        return cfg;
    }

    /**
     * Start a grid with the secondary file system.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void startSecondary() throws Exception {
        FileSystemConfiguration igfsCfg = new FileSystemConfiguration();

        igfsCfg.setDataCacheName("dataCache");
        igfsCfg.setMetaCacheName("metaCache");
        igfsCfg.setName(IGFS_SECONDARY);
        igfsCfg.setBlockSize(SECONDARY_BLOCK_SIZE);
        igfsCfg.setDefaultMode(PRIMARY);
        igfsCfg.setIpcEndpointConfiguration(SECONDARY_REST_CFG);

        CacheConfiguration dataCacheCfg = defaultCacheConfiguration();

        dataCacheCfg.setName("dataCache");
        dataCacheCfg.setCacheMode(PARTITIONED);
        dataCacheCfg.setNearConfiguration(null);
        dataCacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        dataCacheCfg.setAffinityMapper(new IgfsGroupDataBlocksKeyMapper(128));
        dataCacheCfg.setBackups(0);
        dataCacheCfg.setAtomicityMode(TRANSACTIONAL);

        CacheConfiguration metaCacheCfg = defaultCacheConfiguration();

        metaCacheCfg.setName("metaCache");
        metaCacheCfg.setCacheMode(REPLICATED);
        metaCacheCfg.setNearConfiguration(null);
        metaCacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        metaCacheCfg.setAtomicityMode(TRANSACTIONAL);

        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setGridName("grid-secondary");

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(new TcpDiscoveryVmIpFinder(true));

        cfg.setDiscoverySpi(discoSpi);
        cfg.setCacheConfiguration(dataCacheCfg, metaCacheCfg);
        cfg.setFileSystemConfiguration(igfsCfg);

        cfg.setLocalHost("127.0.0.1");

        Ignite g = G.start(cfg);

        igfsSecondary = (IgfsImpl)g.fileSystem(IGFS_SECONDARY);
    }

    /** @throws Exception If failed. */
    public void testMetrics() throws Exception {
        IgfsMetricsTestUtils.testMetrics0(igfsPrimary[0],
            new DefaultIgfsSecondaryFileSystemTestAdapter(igfsSecondary));
    }

    /** @throws Exception If failed. */
    public void testMultipleClose() throws Exception {
        IgfsMetricsTestUtils.testMultipleClose0(igfsPrimary[0]);
    }

    /**
     * Test block metrics.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings({"ResultOfMethodCallIgnored", "ConstantConditions"})
    public void testBlockMetrics() throws Exception {
        IgfsMetricsTestUtils.testBlockMetrics0((IgfsEx)igfsPrimary[0], secFileSys, igfsSecondary, igfsSecondary !=
            null, false);
    }
}