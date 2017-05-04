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

package org.apache.ignite.internal.processors.hadoop.impl.igfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.hadoop.fs.IgniteHadoopIgfsSecondaryFileSystem;
import org.apache.ignite.hadoop.fs.v1.IgniteHadoopFileSystem;
import org.apache.ignite.igfs.IgfsGroupDataBlocksKeyMapper;
import org.apache.ignite.igfs.IgfsIpcEndpointConfiguration;
import org.apache.ignite.igfs.IgfsIpcEndpointType;
import org.apache.ignite.internal.processors.igfs.IgfsCommonAbstractTest;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import java.net.URI;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.igfs.IgfsMode.PRIMARY;

/**
 * Ensures correct modes resolution for SECONDARY paths.
 */
public class IgniteHadoopFileSystemSecondaryFileSystemInitializationSelfTest extends IgfsCommonAbstractTest {
    /** File system. */
    private IgniteHadoopFileSystem fs;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        U.closeQuiet(fs);

        fs = null;

        G.stopAll(true);
    }

    /**
     * Perform initial startup.
     *
     * @param initDfltPathModes WHether to initialize default path modes.
     * @throws Exception If failed.
     */
    @SuppressWarnings({"NullableProblems", "unchecked"})
    private void startUp(boolean initDfltPathModes) throws Exception {
        startUpSecondary();

        FileSystemConfiguration igfsCfg = new FileSystemConfiguration();

        igfsCfg.setDataCacheName("partitioned");
        igfsCfg.setMetaCacheName("replicated");
        igfsCfg.setName("igfs");
        igfsCfg.setBlockSize(512 * 1024);
        igfsCfg.setInitializeDefaultPathModes(initDfltPathModes);

        IgfsIpcEndpointConfiguration endpointCfg = new IgfsIpcEndpointConfiguration();

        endpointCfg.setType(IgfsIpcEndpointType.TCP);
        endpointCfg.setPort(10500);

        igfsCfg.setIpcEndpointConfiguration(endpointCfg);

        igfsCfg.setManagementPort(-1);
        igfsCfg.setSecondaryFileSystem(new IgniteHadoopIgfsSecondaryFileSystem(
            "igfs://igfs-secondary:igfs-grid-secondary@127.0.0.1:11500/",
            "modules/core/src/test/config/hadoop/core-site-loopback-secondary.xml"));

        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setName("partitioned");
        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setNearConfiguration(null);
        cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setAffinityMapper(new IgfsGroupDataBlocksKeyMapper(128));
        cacheCfg.setBackups(0);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);

        CacheConfiguration metaCacheCfg = defaultCacheConfiguration();

        metaCacheCfg.setName("replicated");
        metaCacheCfg.setCacheMode(REPLICATED);
        metaCacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        metaCacheCfg.setAtomicityMode(TRANSACTIONAL);

        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setGridName("igfs-grid");

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(new TcpDiscoveryVmIpFinder(true));

        cfg.setDiscoverySpi(discoSpi);
        cfg.setCacheConfiguration(metaCacheCfg, cacheCfg);
        cfg.setFileSystemConfiguration(igfsCfg);

        cfg.setLocalHost("127.0.0.1");

        G.start(cfg);

        Configuration fsCfg = new Configuration();

        fsCfg.addResource(U.resolveIgniteUrl("modules/core/src/test/config/hadoop/core-site-loopback.xml"));

        fsCfg.setBoolean("fs.igfs.impl.disable.cache", true);

        fs = (IgniteHadoopFileSystem)FileSystem.get(new URI("igfs://igfs:igfs-grid@/"), fsCfg);
    }

    /**
     * Startup secondary file system.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void startUpSecondary() throws Exception {
        FileSystemConfiguration igfsCfg = new FileSystemConfiguration();

        igfsCfg.setDataCacheName("partitioned");
        igfsCfg.setMetaCacheName("replicated");
        igfsCfg.setName("igfs-secondary");
        igfsCfg.setBlockSize(512 * 1024);
        igfsCfg.setDefaultMode(PRIMARY);

        IgfsIpcEndpointConfiguration endpointCfg = new IgfsIpcEndpointConfiguration();

        endpointCfg.setType(IgfsIpcEndpointType.TCP);
        endpointCfg.setPort(11500);

        igfsCfg.setIpcEndpointConfiguration(endpointCfg);

        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setName("partitioned");
        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setNearConfiguration(null);
        cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setAffinityMapper(new IgfsGroupDataBlocksKeyMapper(128));
        cacheCfg.setBackups(0);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);

        CacheConfiguration metaCacheCfg = defaultCacheConfiguration();

        metaCacheCfg.setName("replicated");
        metaCacheCfg.setCacheMode(REPLICATED);
        metaCacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        metaCacheCfg.setAtomicityMode(TRANSACTIONAL);

        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setGridName("igfs-grid-secondary");

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(new TcpDiscoveryVmIpFinder(true));

        cfg.setDiscoverySpi(discoSpi);
        cfg.setCacheConfiguration(metaCacheCfg, cacheCfg);
        cfg.setFileSystemConfiguration(igfsCfg);

        cfg.setLocalHost("127.0.0.1");

        G.start(cfg);
    }

    /**
     * Test scenario when defaults are initialized.
     *
     * @throws Exception If failed.
     */
    public void testDefaultsInitialized() throws Exception {
        check(true);
    }

    /**
     * Test scenario when defaults are not initialized.
     *
     * @throws Exception If failed.
     */
    public void testDefaultsNotInitialized() throws Exception {
        check(false);
    }

    /**
     * Actual check.
     *
     * @param initDfltPathModes Whether to initialize default path modes.
     * @throws Exception If failed.
     */
    private void check(boolean initDfltPathModes) throws Exception {
        startUp(initDfltPathModes);

        assertEquals(initDfltPathModes, fs.hasSecondaryFileSystem());
    }
}