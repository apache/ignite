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

package org.apache.ignite.igfs;

import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.hadoop.fs.IgniteHadoopIgfsSecondaryFileSystem;
import org.apache.ignite.hadoop.fs.v1.IgniteHadoopFileSystem;
import org.apache.ignite.internal.processors.igfs.IgfsCommonAbstractTest;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.igfs.IgfsMode.PRIMARY;
import static org.apache.ignite.igfs.IgfsMode.PROXY;

/**
 * Ensures correct modes resolution for SECONDARY paths.
 */
public class IgniteHadoopFileSystemSecondaryModeSelfTest extends IgfsCommonAbstractTest {
    /** Path to check. */
    private static final Path PATH = new Path("/dir");

    /** Pattern matching the path. */
    private static final String PATTERN_MATCHES = "/dir";

    /** Pattern doesn't matching the path. */
    private static final String PATTERN_NOT_MATCHES = "/files";

    /** Default IGFS mode. */
    private IgfsMode mode;

    /** Path modes. */
    private Map<String, IgfsMode> pathModes;

    /** File system. */
    private IgniteHadoopFileSystem fs;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        mode = null;
        pathModes = null;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        U.closeQuiet(fs);

        fs = null;

        G.stopAll(true);
    }

    /**
     * Perform initial startup.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("NullableProblems")
    private void startUp() throws Exception {
        startUpSecondary();

        FileSystemConfiguration igfsCfg = new FileSystemConfiguration();

        igfsCfg.setDataCacheName("partitioned");
        igfsCfg.setMetaCacheName("replicated");
        igfsCfg.setName("igfs");
        igfsCfg.setBlockSize(512 * 1024);
        igfsCfg.setDefaultMode(mode);
        igfsCfg.setPathModes(pathModes);

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
     * Check path resolution when secondary mode is not default and there are no other exclusion paths.
     *
     * @throws Exception If failed.
     */
    public void testSecondaryNotDefaultNoExclusions() throws Exception {
        mode = PRIMARY;

        startUp();

        assert !secondary(PATH);
        assert !secondary(PATH);
    }

    /**
     * Check path resolution when secondary mode is not default and there is no matching exclusion paths.
     *
     * @throws Exception If failed.
     */
    public void testSecondaryNotDefaultNonMatchingExclusion() throws Exception {
        mode = PRIMARY;

        pathModes(F.t(PATTERN_NOT_MATCHES, PROXY));

        startUp();

        assert !secondary(PATH);
        assert !secondary(PATH);
    }

    /**
     * Check path resolution when secondary mode is not default and there is matching exclusion path.
     *
     * @throws Exception If failed.
     */
    public void testSecondaryNotDefaultMatchingExclusion() throws Exception {
        mode = PRIMARY;

        pathModes(F.t(PATTERN_NOT_MATCHES, PROXY), F.t(PATTERN_MATCHES, PROXY));

        startUp();

        assert secondary(PATH);
        assert secondary(PATH);
    }

    /**
     * Check path resolution when secondary mode is default and there is no exclusion paths.
     *
     * @throws Exception If failed.
     */
    public void testSecondaryDefaultNoExclusions() throws Exception {
        mode = PROXY;

        startUp();

        assert secondary(PATH);
        assert secondary(PATH);
    }

    /**
     * Check path resolution when secondary mode is default and there is no matching exclusion paths.
     *
     * @throws Exception If failed.
     */
    public void testSecondaryDefaultNonMatchingExclusion() throws Exception {
        mode = PROXY;

        pathModes(F.t(PATTERN_NOT_MATCHES, PRIMARY));

        startUp();

        assert secondary(PATH);
        assert secondary(PATH);
    }

    /**
     * Check path resolution when secondary mode is default and there is no matching exclusion paths.
     *
     * @throws Exception If failed.
     */
    public void testSecondaryDefaultMatchingExclusion() throws Exception {
        mode = PROXY;

        pathModes(F.t(PATTERN_NOT_MATCHES, PRIMARY), F.t(PATTERN_MATCHES, PRIMARY));

        startUp();

        assert !secondary(PATH);
        assert !secondary(PATH);
    }

    /**
     * Set IGFS modes for particular paths.
     *
     * @param modes Modes.
     */
    @SafeVarargs
    final void pathModes(IgniteBiTuple<String, IgfsMode>... modes) {
        assert modes != null;

        pathModes = new LinkedHashMap<>(modes.length, 1.0f);

        for (IgniteBiTuple<String, IgfsMode> mode : modes)
            pathModes.put(mode.getKey(), mode.getValue());
    }

    /**
     * Check whether the given path is threaten as SECONDARY in the file system.
     *
     * @param path Path to check.
     * @return {@code True} in case path is secondary.
     * @throws Exception If failed.
     */
    private boolean secondary(Path path) throws Exception {
        return fs.mode(path) == PROXY;
    }
}