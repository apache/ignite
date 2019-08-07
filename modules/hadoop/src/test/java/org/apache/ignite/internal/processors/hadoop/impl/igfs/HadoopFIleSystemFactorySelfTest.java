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

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.hadoop.fs.CachingHadoopFileSystemFactory;
import org.apache.ignite.hadoop.fs.HadoopFileSystemFactory;
import org.apache.ignite.hadoop.fs.IgniteHadoopIgfsSecondaryFileSystem;
import org.apache.ignite.hadoop.fs.v1.IgniteHadoopFileSystem;
import org.apache.ignite.igfs.IgfsGroupDataBlocksKeyMapper;
import org.apache.ignite.igfs.IgfsIpcEndpointConfiguration;
import org.apache.ignite.igfs.IgfsIpcEndpointType;
import org.apache.ignite.igfs.IgfsMode;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.igfs.secondary.IgfsSecondaryFileSystem;
import org.apache.ignite.internal.processors.hadoop.delegate.HadoopDelegateUtils;
import org.apache.ignite.internal.processors.hadoop.delegate.HadoopFileSystemFactoryDelegate;
import org.apache.ignite.internal.processors.igfs.IgfsCommonAbstractTest;
import org.apache.ignite.internal.processors.igfs.IgfsEx;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

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
    @Test
    public void testCustomFactory() throws Exception {
        assert START_CNT.get() == 1;
        assert STOP_CNT.get() == 0;

        // Use IGFS directly.
        primary.mkdirs(IGFS_PATH_DUAL);

        assert primary.exists(IGFS_PATH_DUAL);
        assert secondary.exists(IGFS_PATH_DUAL);

        // Create remote instance.
        FileSystem fs = FileSystem.get(URI.create("igfs://primary@127.0.0.1:10500/"), baseConfiguration());

        assertEquals(1, START_CNT.get());
        assertEquals(0, STOP_CNT.get());

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

        fs.close();

        assertEquals(1, START_CNT.get());
        assertEquals(0, STOP_CNT.get());

        // Stop primary node and ensure that base factory was notified.
        G.stop(primary.context().kernalContext().grid().name(), true);

        assertEquals(1, START_CNT.get());
        assertEquals(1, STOP_CNT.get());
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

        conf.set("fs.defaultFS", "igfs://secondary@127.0.0.1:11500/");

        writeConfigurationToFile(conf);

        // Get file system instance to be used.
        CachingHadoopFileSystemFactory delegate = new CachingHadoopFileSystemFactory();

        delegate.setUri("igfs://secondary@127.0.0.1:11500/");
        delegate.setConfigPaths(SECONDARY_CFG_PATH);

        // Configure factory.
        TestFactory factory = new TestFactory(delegate);

        // Configure file system.
        IgniteHadoopIgfsSecondaryFileSystem secondaryFs = new IgniteHadoopIgfsSecondaryFileSystem();

        secondaryFs.setFileSystemFactory(factory);

        // Start.
        return start("primary", 10500, IgfsMode.DUAL_ASYNC, secondaryFs);
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

        igfsCfg.setName(name);
        igfsCfg.setDefaultMode(dfltMode);
        igfsCfg.setIpcEndpointConfiguration(endpointCfg);
        igfsCfg.setSecondaryFileSystem(secondaryFs);

        CacheConfiguration dataCacheCfg = defaultCacheConfiguration();

        dataCacheCfg.setCacheMode(PARTITIONED);
        dataCacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        dataCacheCfg.setAffinityMapper(new IgfsGroupDataBlocksKeyMapper(2));
        dataCacheCfg.setBackups(0);
        dataCacheCfg.setAtomicityMode(TRANSACTIONAL);

        CacheConfiguration metaCacheCfg = defaultCacheConfiguration();

        metaCacheCfg.setCacheMode(REPLICATED);
        metaCacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        metaCacheCfg.setAtomicityMode(TRANSACTIONAL);

        igfsCfg.setDataCacheConfiguration(dataCacheCfg);
        igfsCfg.setMetaCacheConfiguration(metaCacheCfg);

        if (secondaryFs != null) {
            Map<String, IgfsMode> modes = new HashMap<>();
            modes.put("/ignite/sync/", IgfsMode.DUAL_SYNC);
            modes.put("/ignite/async/", IgfsMode.DUAL_ASYNC);
            modes.put("/ignite/proxy/", IgfsMode.PROXY);

            igfsCfg.setPathModes(modes);
        }

        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setIgniteInstanceName(name);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(new TcpDiscoveryVmIpFinder(true));

        cfg.setDiscoverySpi(discoSpi);
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
    private static class TestFactory implements HadoopFileSystemFactory, LifecycleAware {
        /** */
        private static final long serialVersionUID = 0L;

        /** File system factory. */
        private CachingHadoopFileSystemFactory factory;

        /** File system. */
        private transient HadoopFileSystemFactoryDelegate delegate;

        /**
         * Constructor.
         *
         * @param factory File system factory.
         */
        public TestFactory(CachingHadoopFileSystemFactory factory) {
            this.factory = factory;
        }

        /** {@inheritDoc} */
        @Override public Object get(String usrName) throws IOException {
            return delegate.get(usrName);
        }

        /** {@inheritDoc} */
        @Override public void start() throws IgniteException {
            delegate = HadoopDelegateUtils.fileSystemFactoryDelegate(getClass().getClassLoader(), factory);

            delegate.start();

            START_CNT.incrementAndGet();
        }

        /** {@inheritDoc} */
        @Override public void stop() throws IgniteException {
            STOP_CNT.incrementAndGet();
        }
    }
}
