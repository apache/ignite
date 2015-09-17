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

import java.lang.reflect.Field;
import java.net.URI;
import java.nio.file.Paths;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.hadoop.fs.v1.IgniteHadoopFileSystem;
import org.apache.ignite.internal.igfs.common.IgfsLogger;
import org.apache.ignite.internal.processors.igfs.IgfsCommonAbstractTest;
import org.apache.ignite.internal.processors.igfs.IgfsEx;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.igfs.IgfsMode.PRIMARY;
import static org.apache.ignite.internal.processors.hadoop.fs.HadoopParameters.PARAM_IGFS_LOG_DIR;
import static org.apache.ignite.internal.processors.hadoop.fs.HadoopParameters.PARAM_IGFS_LOG_ENABLED;

/**
 * Ensures that sampling is really turned on/off.
 */
public class IgniteHadoopFileSystemLoggerStateSelfTest extends IgfsCommonAbstractTest {
    /** IGFS. */
    private IgfsEx igfs;

    /** File system. */
    private FileSystem fs;

    /** Whether logging is enabled in FS configuration. */
    private boolean logging;

    /** whether sampling is enabled. */
    private Boolean sampling;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        U.closeQuiet(fs);

        igfs = null;
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
        FileSystemConfiguration igfsCfg = new FileSystemConfiguration();

        igfsCfg.setDataCacheName("partitioned");
        igfsCfg.setMetaCacheName("replicated");
        igfsCfg.setName("igfs");
        igfsCfg.setBlockSize(512 * 1024);
        igfsCfg.setDefaultMode(PRIMARY);

        IgfsIpcEndpointConfiguration endpointCfg = new IgfsIpcEndpointConfiguration();

        endpointCfg.setType(IgfsIpcEndpointType.TCP);
        endpointCfg.setPort(10500);

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

        cfg.setGridName("igfs-grid");

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(new TcpDiscoveryVmIpFinder(true));

        cfg.setDiscoverySpi(discoSpi);
        cfg.setCacheConfiguration(metaCacheCfg, cacheCfg);
        cfg.setFileSystemConfiguration(igfsCfg);

        cfg.setLocalHost("127.0.0.1");
        cfg.setConnectorConfiguration(null);

        Ignite g = G.start(cfg);

        igfs = (IgfsEx)g.fileSystem("igfs");

        igfs.globalSampling(sampling);

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
        igfs.globalSampling(true);

        fs = fileSystem();

        assert logEnabled();

        fs.close();

        // True => "not set" transition.
        igfs.globalSampling(null);

        fs = fileSystem();

        assert !logEnabled();

        // "Not-set" => false transition.
        igfs.globalSampling(false);

        fs = fileSystem();

        assert !logEnabled();

        fs.close();

        // False => "not=set" transition.
        igfs.globalSampling(null);

        fs = fileSystem();

        assert !logEnabled();

        fs.close();

        // True => false transition.
        igfs.globalSampling(true);
        igfs.globalSampling(false);

        fs = fileSystem();

        assert !logEnabled();

        fs.close();

        // False => true transition.
        igfs.globalSampling(true);

        fs = fileSystem();

        assert logEnabled();
    }

    /**
     * Ensure that log directory is set to IGFS when client FS connects.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("ConstantConditions")
    public void testLogDirectory() throws Exception {
        startUp();

        assertEquals(Paths.get(U.getIgniteHome()).normalize().toString(),
            igfs.clientLogDirectory());
    }

    /**
     * Instantiate new file system.
     *
     * @return New file system.
     * @throws Exception If failed.
     */
    private IgniteHadoopFileSystem fileSystem() throws Exception {
        Configuration fsCfg = new Configuration();

        fsCfg.addResource(U.resolveIgniteUrl("modules/core/src/test/config/hadoop/core-site-loopback.xml"));

        fsCfg.setBoolean("fs.igfs.impl.disable.cache", true);

        if (logging)
            fsCfg.setBoolean(String.format(PARAM_IGFS_LOG_ENABLED, "igfs:igfs-grid@"), logging);

        fsCfg.setStrings(String.format(PARAM_IGFS_LOG_DIR, "igfs:igfs-grid@"), U.getIgniteHome());

        return (IgniteHadoopFileSystem)FileSystem.get(new URI("igfs://igfs:igfs-grid@/"), fsCfg);
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

        return ((IgfsLogger)field.get(fs)).isLogEnabled();
    }
}