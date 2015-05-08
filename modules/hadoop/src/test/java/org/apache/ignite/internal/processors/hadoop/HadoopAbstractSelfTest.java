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

package org.apache.ignite.internal.processors.hadoop;

import org.apache.hadoop.conf.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.igfs.*;
import org.apache.ignite.hadoop.fs.v2.IgniteHadoopFileSystem;
import org.apache.ignite.internal.processors.hadoop.fs.*;
import org.apache.ignite.spi.communication.tcp.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;

import java.io.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;

/**
 * Abstract class for Hadoop tests.
 */
public abstract class HadoopAbstractSelfTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** REST port. */
    protected static final int REST_PORT = 11212;

    /** IGFS name. */
    protected static final String igfsName = null;

    /** IGFS name. */
    protected static final String igfsMetaCacheName = "meta";

    /** IGFS name. */
    protected static final String igfsDataCacheName = "data";

    /** IGFS block size. */
    protected static final int igfsBlockSize = 1024;

    /** IGFS block group size. */
    protected static final int igfsBlockGroupSize = 8;

    /** Initial REST port. */
    private int restPort = REST_PORT;

    /** Initial classpath. */
    private static String initCp;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        // Add surefire classpath to regular classpath.
        initCp = System.getProperty("java.class.path");

        String surefireCp = System.getProperty("surefire.test.class.path");

        if (surefireCp != null)
            System.setProperty("java.class.path", initCp + File.pathSeparatorChar + surefireCp);

        super.beforeTestsStarted();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        // Restore classpath.
        System.setProperty("java.class.path", initCp);

        initCp = null;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setHadoopConfiguration(hadoopConfiguration(gridName));

        TcpDiscoverySpi discoSpi = (TcpDiscoverySpi)cfg.getDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        if (igfsEnabled()) {
            cfg.setCacheConfiguration(metaCacheConfiguration(), dataCacheConfiguration());

            cfg.setFileSystemConfiguration(igfsConfiguration());
        }

        if (restEnabled()) {
            ConnectorConfiguration clnCfg = new ConnectorConfiguration();

            clnCfg.setPort(restPort++);

            cfg.setConnectorConfiguration(clnCfg);
        }

        cfg.setLocalHost("127.0.0.1");
        cfg.setPeerClassLoadingEnabled(false);

        return cfg;
    }

    /**
     * @param gridName Grid name.
     * @return Hadoop configuration.
     */
    public HadoopConfiguration hadoopConfiguration(String gridName) {
        HadoopConfiguration cfg = new HadoopConfiguration();

        cfg.setMaxParallelTasks(3);

        return cfg;
    }

    /**
     * @return IGFS configuration.
     */
    public FileSystemConfiguration igfsConfiguration() {
        FileSystemConfiguration cfg = new FileSystemConfiguration();

        cfg.setName(igfsName);
        cfg.setBlockSize(igfsBlockSize);
        cfg.setDataCacheName(igfsDataCacheName);
        cfg.setMetaCacheName(igfsMetaCacheName);
        cfg.setFragmentizerEnabled(false);

        return cfg;
    }

    /**
     * @return IGFS meta cache configuration.
     */
    public CacheConfiguration metaCacheConfiguration() {
        CacheConfiguration cfg = new CacheConfiguration();

        cfg.setName(igfsMetaCacheName);
        cfg.setCacheMode(REPLICATED);
        cfg.setAtomicityMode(TRANSACTIONAL);
        cfg.setWriteSynchronizationMode(FULL_SYNC);

        return cfg;
    }

    /**
     * @return IGFS data cache configuration.
     */
    private CacheConfiguration dataCacheConfiguration() {
        CacheConfiguration cfg = new CacheConfiguration();

        cfg.setName(igfsDataCacheName);
        cfg.setCacheMode(PARTITIONED);
        cfg.setAtomicityMode(TRANSACTIONAL);
        cfg.setAffinityMapper(new IgfsGroupDataBlocksKeyMapper(igfsBlockGroupSize));
        cfg.setWriteSynchronizationMode(FULL_SYNC);

        return cfg;
    }

    /**
     * @return {@code True} if IGFS is enabled on Hadoop nodes.
     */
    protected boolean igfsEnabled() {
        return false;
    }

    /**
     * @return {@code True} if REST is enabled on Hadoop nodes.
     */
    protected boolean restEnabled() {
        return false;
    }

    /**
     * @return Number of nodes to start.
     */
    protected int gridCount() {
        return 3;
    }

    /**
     * @param cfg Config.
     */
    protected void setupFileSystems(Configuration cfg) {
        cfg.set("fs.defaultFS", igfsScheme());
        cfg.set("fs.igfs.impl", org.apache.ignite.hadoop.fs.v1.IgniteHadoopFileSystem.class.getName());
        cfg.set("fs.AbstractFileSystem.igfs.impl", IgniteHadoopFileSystem.
            class.getName());

        HadoopFileSystemsUtils.setupFileSystems(cfg);
    }

    /**
     * @return IGFS scheme for test.
     */
    protected String igfsScheme() {
        return "igfs://:" + getTestGridName(0) + "@/";
    }
}
