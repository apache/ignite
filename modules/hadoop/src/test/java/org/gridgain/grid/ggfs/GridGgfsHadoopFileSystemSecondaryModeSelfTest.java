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

package org.gridgain.grid.ggfs;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.fs.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.ggfs.hadoop.v1.*;
import org.gridgain.grid.kernal.ggfs.hadoop.*;
import org.gridgain.grid.kernal.processors.ggfs.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import java.net.*;
import java.util.*;

import static org.apache.ignite.cache.GridCacheAtomicityMode.*;
import static org.apache.ignite.cache.GridCacheMode.*;
import static org.apache.ignite.fs.IgniteFsMode.*;

/**
 * Ensures correct modes resolution for SECONDARY paths.
 */
public class GridGgfsHadoopFileSystemSecondaryModeSelfTest extends GridGgfsCommonAbstractTest {
    /** Path to check. */
    private static final Path PATH = new Path("/dir");

    /** Pattern matching the path. */
    private static final String PATTERN_MATCHES = "/dir";

    /** Pattern doesn't matching the path. */
    private static final String PATTERN_NOT_MATCHES = "/files";

    /** Default GGFS mode. */
    private IgniteFsMode mode;

    /** Path modes. */
    private Map<String, IgniteFsMode> pathModes;

    /** File system. */
    private GridGgfsHadoopFileSystem fs;

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

        IgniteFsConfiguration ggfsCfg = new IgniteFsConfiguration();

        ggfsCfg.setDataCacheName("partitioned");
        ggfsCfg.setMetaCacheName("replicated");
        ggfsCfg.setName("ggfs");
        ggfsCfg.setBlockSize(512 * 1024);
        ggfsCfg.setDefaultMode(mode);
        ggfsCfg.setPathModes(pathModes);
        ggfsCfg.setIpcEndpointConfiguration(new HashMap<String, String>() {{
            put("type", "tcp");
            put("port", "10500");
        }});

        ggfsCfg.setManagementPort(-1);
        ggfsCfg.setSecondaryFileSystem(new GridGgfsHadoopFileSystemWrapper(
            "ggfs://ggfs-secondary:ggfs-grid-secondary@127.0.0.1:11500/",
            "modules/core/src/test/config/hadoop/core-site-loopback-secondary.xml"));

        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setName("partitioned");
        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setDistributionMode(GridCacheDistributionMode.PARTITIONED_ONLY);
        cacheCfg.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setAffinityMapper(new IgniteFsGroupDataBlocksKeyMapper(128));
        cacheCfg.setBackups(0);
        cacheCfg.setQueryIndexEnabled(false);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);

        CacheConfiguration metaCacheCfg = defaultCacheConfiguration();

        metaCacheCfg.setName("replicated");
        metaCacheCfg.setCacheMode(REPLICATED);
        metaCacheCfg.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);
        metaCacheCfg.setQueryIndexEnabled(false);
        metaCacheCfg.setAtomicityMode(TRANSACTIONAL);

        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setGridName("ggfs-grid");

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(new TcpDiscoveryVmIpFinder(true));

        cfg.setDiscoverySpi(discoSpi);
        cfg.setCacheConfiguration(metaCacheCfg, cacheCfg);
        cfg.setGgfsConfiguration(ggfsCfg);

        cfg.setLocalHost("127.0.0.1");

        G.start(cfg);

        Configuration fsCfg = new Configuration();

        fsCfg.addResource(U.resolveGridGainUrl("modules/core/src/test/config/hadoop/core-site-loopback.xml"));

        fsCfg.setBoolean("fs.ggfs.impl.disable.cache", true);

        fs = (GridGgfsHadoopFileSystem)FileSystem.get(new URI("ggfs://ggfs:ggfs-grid@/"), fsCfg);
    }

    /**
     * Startup secondary file system.
     *
     * @throws Exception If failed.
     */
    private void startUpSecondary() throws Exception {
        IgniteFsConfiguration ggfsCfg = new IgniteFsConfiguration();

        ggfsCfg.setDataCacheName("partitioned");
        ggfsCfg.setMetaCacheName("replicated");
        ggfsCfg.setName("ggfs-secondary");
        ggfsCfg.setBlockSize(512 * 1024);
        ggfsCfg.setDefaultMode(PRIMARY);
        ggfsCfg.setIpcEndpointConfiguration(new HashMap<String, String>() {{
            put("type", "tcp");
            put("port", "11500");
        }});

        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setName("partitioned");
        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setDistributionMode(GridCacheDistributionMode.PARTITIONED_ONLY);
        cacheCfg.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setAffinityMapper(new IgniteFsGroupDataBlocksKeyMapper(128));
        cacheCfg.setBackups(0);
        cacheCfg.setQueryIndexEnabled(false);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);

        CacheConfiguration metaCacheCfg = defaultCacheConfiguration();

        metaCacheCfg.setName("replicated");
        metaCacheCfg.setCacheMode(REPLICATED);
        metaCacheCfg.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);
        metaCacheCfg.setQueryIndexEnabled(false);
        metaCacheCfg.setAtomicityMode(TRANSACTIONAL);

        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setGridName("ggfs-grid-secondary");

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(new TcpDiscoveryVmIpFinder(true));

        cfg.setDiscoverySpi(discoSpi);
        cfg.setCacheConfiguration(metaCacheCfg, cacheCfg);
        cfg.setGgfsConfiguration(ggfsCfg);

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
     * Set GGFS modes for particular paths.
     *
     * @param modes Modes.
     */
    @SafeVarargs
    final void pathModes(IgniteBiTuple<String, IgniteFsMode>... modes) {
        assert modes != null;

        pathModes = new LinkedHashMap<>(modes.length, 1.0f);

        for (IgniteBiTuple<String, IgniteFsMode> mode : modes)
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
