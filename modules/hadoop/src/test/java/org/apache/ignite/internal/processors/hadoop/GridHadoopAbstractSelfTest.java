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
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.ignitefs.*;
import org.apache.ignite.internal.processors.hadoop.fs.*;
import org.apache.ignite.spi.communication.tcp.*;
import org.apache.ignite.testframework.junits.common.*;

import java.io.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;

/**
 * Abstract class for Hadoop tests.
 */
public abstract class GridHadoopAbstractSelfTest extends GridCommonAbstractTest {
    /** REST port. */
    protected static final int REST_PORT = 11212;

    /** GGFS name. */
    protected static final String ggfsName = null;

    /** GGFS name. */
    protected static final String ggfsMetaCacheName = "meta";

    /** GGFS name. */
    protected static final String ggfsDataCacheName = "data";

    /** GGFS block size. */
    protected static final int ggfsBlockSize = 1024;

    /** GGFS block group size. */
    protected static final int ggfsBlockGroupSize = 8;

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

        TcpCommunicationSpi commSpi = new TcpCommunicationSpi();

        commSpi.setSharedMemoryPort(-1);

        cfg.setCommunicationSpi(commSpi);

        if (ggfsEnabled()) {
            cfg.setCacheConfiguration(metaCacheConfiguration(), dataCacheConfiguration());

            cfg.setGgfsConfiguration(ggfsConfiguration());
        }

        if (restEnabled()) {
            cfg.setRestEnabled(true);
            cfg.setRestTcpPort(restPort++);
        }

        cfg.setLocalHost("127.0.0.1");
        cfg.setPeerClassLoadingEnabled(false);

        return cfg;
    }

    /**
     * @param gridName Grid name.
     * @return Hadoop configuration.
     */
    public GridHadoopConfiguration hadoopConfiguration(String gridName) {
        GridHadoopConfiguration cfg = new GridHadoopConfiguration();

        cfg.setMaxParallelTasks(3);

        return cfg;
    }

    /**
     * @return GGFS configuration.
     */
    public IgniteFsConfiguration ggfsConfiguration() {
        IgniteFsConfiguration cfg = new IgniteFsConfiguration();

        cfg.setName(ggfsName);
        cfg.setBlockSize(ggfsBlockSize);
        cfg.setDataCacheName(ggfsDataCacheName);
        cfg.setMetaCacheName(ggfsMetaCacheName);
        cfg.setFragmentizerEnabled(false);

        return cfg;
    }

    /**
     * @return GGFS meta cache configuration.
     */
    public CacheConfiguration metaCacheConfiguration() {
        CacheConfiguration cfg = new CacheConfiguration();

        cfg.setName(ggfsMetaCacheName);
        cfg.setCacheMode(REPLICATED);
        cfg.setAtomicityMode(TRANSACTIONAL);
        cfg.setWriteSynchronizationMode(FULL_SYNC);

        return cfg;
    }

    /**
     * @return GGFS data cache configuration.
     */
    private CacheConfiguration dataCacheConfiguration() {
        CacheConfiguration cfg = new CacheConfiguration();

        cfg.setName(ggfsDataCacheName);
        cfg.setCacheMode(PARTITIONED);
        cfg.setAtomicityMode(TRANSACTIONAL);
        cfg.setAffinityMapper(new IgniteFsGroupDataBlocksKeyMapper(ggfsBlockGroupSize));
        cfg.setWriteSynchronizationMode(FULL_SYNC);

        return cfg;
    }

    /**
     * @return {@code True} if GGFS is enabled on Hadoop nodes.
     */
    protected boolean ggfsEnabled() {
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
        cfg.set("fs.defaultFS", ggfsScheme());
        cfg.set("fs.ggfs.impl", org.apache.ignite.ignitefs.hadoop.v1.GridGgfsHadoopFileSystem.class.getName());
        cfg.set("fs.AbstractFileSystem.ggfs.impl", org.apache.ignite.ignitefs.hadoop.v2.GridGgfsHadoopFileSystem.
            class.getName());

        GridHadoopFileSystemsUtils.setupFileSystems(cfg);
    }

    /**
     * @return GGFS scheme for test.
     */
    protected String ggfsScheme() {
        return "ggfs://:" + getTestGridName(0) + "@/";
    }
}
