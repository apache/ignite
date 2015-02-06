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

package org.apache.ignite.ignitefs;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.ignitefs.hadoop.v1.*;
import org.apache.ignite.internal.fs.common.*;
import org.apache.ignite.internal.processors.fs.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;

import java.lang.reflect.*;
import java.net.*;
import java.nio.file.*;
import java.util.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.ignitefs.IgniteFsMode.*;
import static org.apache.ignite.ignitefs.hadoop.GridGgfsHadoopParameters.*;

/**
 * Ensures that sampling is really turned on/off.
 */
public class GridGgfsHadoopFileSystemLoggerStateSelfTest extends GridGgfsCommonAbstractTest {
    /** GGFS. */
    private GridGgfsEx ggfs;

    /** File system. */
    private FileSystem fs;

    /** Whether logging is enabled in FS configuration. */
    private boolean logging;

    /** whether sampling is enabled. */
    private Boolean sampling;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        U.closeQuiet(fs);

        ggfs = null;
        fs = null;

        G.stopAll(true);

        logging = false;
        sampling = null;
    }

    /**
     * Startup the grid and instantiate the file system.
     *
     * @throws Exception If failed.
     */
    private void startUp() throws Exception {
        IgniteFsConfiguration ggfsCfg = new IgniteFsConfiguration();

        ggfsCfg.setDataCacheName("partitioned");
        ggfsCfg.setMetaCacheName("replicated");
        ggfsCfg.setName("ggfs");
        ggfsCfg.setBlockSize(512 * 1024);
        ggfsCfg.setDefaultMode(PRIMARY);
        ggfsCfg.setIpcEndpointConfiguration(new HashMap<String, String>() {{
            put("type", "tcp");
            put("port", "10500");
        }});

        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setName("partitioned");
        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setDistributionMode(CacheDistributionMode.PARTITIONED_ONLY);
        cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setAffinityMapper(new IgniteFsGroupDataBlocksKeyMapper(128));
        cacheCfg.setBackups(0);
        cacheCfg.setQueryIndexEnabled(false);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);

        CacheConfiguration metaCacheCfg = defaultCacheConfiguration();

        metaCacheCfg.setName("replicated");
        metaCacheCfg.setCacheMode(REPLICATED);
        metaCacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
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
        cfg.setRestEnabled(false);

        Ignite g = G.start(cfg);

        ggfs = (GridGgfsEx)g.fileSystem("ggfs");

        ggfs.globalSampling(sampling);

        fs = fileSystem();
    }

    /**
     * When logging is disabled and sampling is not set no-op logger must be used.
     *
     * @throws Exception If failed.
     */
    public void testLoggingDisabledSamplingNotSet() throws Exception {
        startUp();

        assert !logEnabled();
    }

    /**
     * When logging is enabled and sampling is not set file logger must be used.
     *
     * @throws Exception If failed.
     */
    public void testLoggingEnabledSamplingNotSet() throws Exception {
        logging = true;

        startUp();

        assert logEnabled();
    }

    /**
     * When logging is disabled and sampling is disabled no-op logger must be used.
     *
     * @throws Exception If failed.
     */
    public void testLoggingDisabledSamplingDisabled() throws Exception {
        sampling = false;

        startUp();

        assert !logEnabled();
    }

    /**
     * When logging is enabled and sampling is disabled no-op logger must be used.
     *
     * @throws Exception If failed.
     */
    public void testLoggingEnabledSamplingDisabled() throws Exception {
        logging = true;
        sampling = false;

        startUp();

        assert !logEnabled();
    }

    /**
     * When logging is disabled and sampling is enabled file logger must be used.
     *
     * @throws Exception If failed.
     */
    public void testLoggingDisabledSamplingEnabled() throws Exception {
        sampling = true;

        startUp();

        assert logEnabled();
    }

    /**
     * When logging is enabled and sampling is enabled file logger must be used.
     *
     * @throws Exception If failed.
     */
    public void testLoggingEnabledSamplingEnabled() throws Exception {
        logging = true;
        sampling = true;

        startUp();

        assert logEnabled();
    }

    /**
     * Ensure sampling change through API causes changes in logging on subsequent client connections.
     *
     * @throws Exception If failed.
     */
    public void testSamplingChange() throws Exception {
        // Start with sampling not set.
        startUp();

        assert !logEnabled();

        fs.close();

        // "Not set" => true transition.
        ggfs.globalSampling(true);

        fs = fileSystem();

        assert logEnabled();

        fs.close();

        // True => "not set" transition.
        ggfs.globalSampling(null);

        fs = fileSystem();

        assert !logEnabled();

        // "Not-set" => false transition.
        ggfs.globalSampling(false);

        fs = fileSystem();

        assert !logEnabled();

        fs.close();

        // False => "not=set" transition.
        ggfs.globalSampling(null);

        fs = fileSystem();

        assert !logEnabled();

        fs.close();

        // True => false transition.
        ggfs.globalSampling(true);
        ggfs.globalSampling(false);

        fs = fileSystem();

        assert !logEnabled();

        fs.close();

        // False => true transition.
        ggfs.globalSampling(true);

        fs = fileSystem();

        assert logEnabled();
    }

    /**
     * Ensure that log directory is set to GGFS when client FS connects.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("ConstantConditions")
    public void testLogDirectory() throws Exception {
        startUp();

        assertEquals(Paths.get(U.getIgniteHome()).normalize().toString(),
            ggfs.clientLogDirectory());
    }

    /**
     * Instantiate new file system.
     *
     * @return New file system.
     * @throws Exception If failed.
     */
    private GridGgfsHadoopFileSystem fileSystem() throws Exception {
        Configuration fsCfg = new Configuration();

        fsCfg.addResource(U.resolveIgniteUrl("modules/core/src/test/config/hadoop/core-site-loopback.xml"));

        fsCfg.setBoolean("fs.ggfs.impl.disable.cache", true);

        if (logging)
            fsCfg.setBoolean(String.format(PARAM_GGFS_LOG_ENABLED, "ggfs:ggfs-grid@"), logging);

        fsCfg.setStrings(String.format(PARAM_GGFS_LOG_DIR, "ggfs:ggfs-grid@"), U.getIgniteHome());

        return (GridGgfsHadoopFileSystem)FileSystem.get(new URI("ggfs://ggfs:ggfs-grid@/"), fsCfg);
    }

    /**
     * Ensure that real logger is used by the file system.
     *
     * @return {@code True} in case path is secondary.
     * @throws Exception If failed.
     */
    private boolean logEnabled() throws Exception {
        assert fs != null;

        Field field = fs.getClass().getDeclaredField("clientLog");

        field.setAccessible(true);

        return ((GridGgfsLogger)field.get(fs)).isLogEnabled();
    }
}
