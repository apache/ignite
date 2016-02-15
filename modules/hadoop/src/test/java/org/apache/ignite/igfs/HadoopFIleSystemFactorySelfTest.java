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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.hadoop.fs.CachingHadoopFileSystemFactory;
import org.apache.ignite.hadoop.fs.IgniteHadoopIgfsSecondaryFileSystem;
import org.apache.ignite.hadoop.fs.v1.IgniteHadoopFileSystem;
import org.apache.ignite.igfs.secondary.IgfsSecondaryFileSystem;
import org.apache.ignite.internal.processors.igfs.IgfsCommonAbstractTest;
import org.apache.ignite.internal.processors.igfs.IgfsEx;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;
import java.io.Externalizable;

import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 * Tests for Hadoop file system factory.
 */
public class HadoopFIleSystemFactorySelfTest extends IgfsCommonAbstractTest {
    /** Amount of "start" invocations */
    private static final AtomicInteger START_CNT = new AtomicInteger();

    /** Amount of "stop" invocations */
    private static final AtomicInteger STOP_CNT = new AtomicInteger();

    /** Path to secondary file system configuration. */
    private static final String SECONDARY_CFG_PATH = "/work/core-site-HadoopFIleSystemFactorySelfTest.xml";

    /** IGFS path for DUAL mode. */
    private static final Path PATH_DUAL = new Path("/ignite/sync/test_dir");

    /** IGFS path for PROXY mode. */
    private static final Path PATH_PROXY = new Path("/ignite/proxy/test_dir");

    /** IGFS path for DUAL mode. */
    private static final IgfsPath IGFS_PATH_DUAL = new IgfsPath("/ignite/sync/test_dir");

    /** IGFS path for PROXY mode. */
    private static final IgfsPath IGFS_PATH_PROXY = new IgfsPath("/ignite/proxy/test_dir");

    /** Secondary IGFS. */
    private IgfsEx secondary;

    /** Primary IGFS. */
    private IgfsEx primary;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        START_CNT.set(0);
        STOP_CNT.set(0);

        secondary = startSecondary();
        primary = startPrimary();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        secondary = null;
        primary = null;

        stopAllGrids();
    }

    /**
     * Test custom factory.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void testCustomFactory() throws Exception {
        assert START_CNT.get() == 1;
        assert STOP_CNT.get() == 0;

        // Use IGFS directly.
        primary.mkdirs(IGFS_PATH_DUAL);

        assert primary.exists(IGFS_PATH_DUAL);
        assert secondary.exists(IGFS_PATH_DUAL);

        GridTestUtils.assertThrows(null, new Callable<Object>() {
            @Override public Object call() throws Exception {
                primary.mkdirs(IGFS_PATH_PROXY);

                return null;
            }
        }, IgfsInvalidPathException.class, null);

        // Create remote instance.
        FileSystem fs = FileSystem.get(URI.create("igfs://primary:primary@127.0.0.1:10500/"), baseConfiguration());

        // Ensure lifecycle callback was invoked.
        assert START_CNT.get() == 2;
        assert STOP_CNT.get() == 0;

        // Check file system operations.
        assert fs.exists(PATH_DUAL);

        assert fs.delete(PATH_DUAL, true);
        assert !primary.exists(IGFS_PATH_DUAL);
        assert !secondary.exists(IGFS_PATH_DUAL);
        assert !fs.exists(PATH_DUAL);

        assert fs.mkdirs(PATH_DUAL);
        assert primary.exists(IGFS_PATH_DUAL);
        assert secondary.exists(IGFS_PATH_DUAL);
        assert fs.exists(PATH_DUAL);

        assert fs.mkdirs(PATH_PROXY);
        assert secondary.exists(IGFS_PATH_PROXY);
        assert fs.exists(PATH_PROXY);

        // Close file system and ensure that associated factory was notified.
        fs.close();

        assert START_CNT.get() == 2;
        assert STOP_CNT.get() == 1;

        // Stop primary node and ensure that base factory was notified.
        G.stop(primary.context().kernalContext().grid().name(), true);

        assert START_CNT.get() == 2;
        assert STOP_CNT.get() == 2;
    }

    /**
     * Start secondary IGFS.
     *
     * @return IGFS.
     * @throws Exception If failed.
     */
    private static IgfsEx startSecondary() throws Exception {
        return start("secondary", 11500, IgfsMode.PRIMARY, null);
    }

    /**
     * Start primary IGFS.
     *
     * @return IGFS.
     * @throws Exception If failed.
     */
    private static IgfsEx startPrimary() throws Exception {
        // Prepare configuration.
        Configuration conf = baseConfiguration();

        conf.set("fs.defaultFS", "igfs://secondary:secondary@127.0.0.1:11500/");

        writeConfigurationToFile(conf);

        // Configure factory.
        TestFactory factory = new TestFactory();

        factory.setUri("igfs://secondary:secondary@127.0.0.1:11500/");
        factory.setConfigPaths(SECONDARY_CFG_PATH);

        // Configure file system.
        IgniteHadoopIgfsSecondaryFileSystem fs = new IgniteHadoopIgfsSecondaryFileSystem();

        fs.setFileSystemFactory(factory);

        // Start.
        return start("primary", 10500, IgfsMode.PRIMARY, fs);
    }

    /**
     * Start Ignite node with IGFS instance.
     *
     * @param name Node and IGFS name.
     * @param endpointPort Endpoint port.
     * @param dfltMode Default path mode.
     * @param secondaryFs Secondary file system.
     * @return Igfs instance.
     */
    private static IgfsEx start(String name, int endpointPort, IgfsMode dfltMode,
        @Nullable IgfsSecondaryFileSystem secondaryFs) {
        IgfsIpcEndpointConfiguration endpointCfg = new IgfsIpcEndpointConfiguration();

        endpointCfg.setType(IgfsIpcEndpointType.TCP);
        endpointCfg.setHost("127.0.0.1");
        endpointCfg.setPort(endpointPort);

        FileSystemConfiguration igfsCfg = new FileSystemConfiguration();

        igfsCfg.setDataCacheName("dataCache");
        igfsCfg.setMetaCacheName("metaCache");
        igfsCfg.setName(name);
        igfsCfg.setDefaultMode(dfltMode);
        igfsCfg.setIpcEndpointConfiguration(endpointCfg);
        igfsCfg.setSecondaryFileSystem(secondaryFs);

        CacheConfiguration dataCacheCfg = defaultCacheConfiguration();

        dataCacheCfg.setName("dataCache");
        dataCacheCfg.setCacheMode(PARTITIONED);
        dataCacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        dataCacheCfg.setAffinityMapper(new IgfsGroupDataBlocksKeyMapper(2));
        dataCacheCfg.setBackups(0);
        dataCacheCfg.setAtomicityMode(TRANSACTIONAL);
        dataCacheCfg.setOffHeapMaxMemory(0);

        CacheConfiguration metaCacheCfg = defaultCacheConfiguration();

        metaCacheCfg.setName("metaCache");
        metaCacheCfg.setCacheMode(REPLICATED);
        metaCacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        metaCacheCfg.setAtomicityMode(TRANSACTIONAL);

        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setGridName(name);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(new TcpDiscoveryVmIpFinder(true));

        cfg.setDiscoverySpi(discoSpi);
        cfg.setCacheConfiguration(dataCacheCfg, metaCacheCfg);
        cfg.setFileSystemConfiguration(igfsCfg);

        cfg.setLocalHost("127.0.0.1");
        cfg.setConnectorConfiguration(null);

        return (IgfsEx)G.start(cfg).fileSystem(name);
    }

    /**
     * Create base FileSystem configuration.
     *
     * @return Configuration.
     */
    private static Configuration baseConfiguration() {
        Configuration conf = new Configuration();

        conf.set("fs.igfs.impl", IgniteHadoopFileSystem.class.getName());

        return conf;
    }

    /**
     * Write configuration to file.
     *
     * @param conf Configuration.
     * @throws Exception If failed.
     */
    @SuppressWarnings("ResultOfMethodCallIgnored")
    private static void writeConfigurationToFile(Configuration conf) throws Exception {
        final String path = U.getIgniteHome() + SECONDARY_CFG_PATH;

        File file = new File(path);

        file.delete();

        assertFalse(file.exists());

        try (FileOutputStream fos = new FileOutputStream(file)) {
            conf.writeXml(fos);
        }

        assertTrue(file.exists());
    }

    /**
     * Test factory.
     */
    private static class TestFactory extends CachingHadoopFileSystemFactory {
        /**
         * {@link Externalizable} support.
         */
        public TestFactory() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void start() throws IgniteException {
            START_CNT.incrementAndGet();

            super.start();
        }

        /** {@inheritDoc} */
        @Override public void stop() throws IgniteException {
            STOP_CNT.incrementAndGet();

            super.stop();
        }
    }
}
