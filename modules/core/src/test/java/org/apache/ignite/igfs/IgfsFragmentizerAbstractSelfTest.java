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

import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.igfs.IgfsCommonAbstractTest;
import org.apache.ignite.internal.processors.igfs.IgfsEx;
import org.apache.ignite.internal.processors.igfs.IgfsFileInfo;
import org.apache.ignite.internal.processors.igfs.IgfsMetaManager;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 * Fragmentizer abstract self test.
 */
public class IgfsFragmentizerAbstractSelfTest extends IgfsCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Test nodes count. */
    protected static final int NODE_CNT = 4;

    /** IGFS block size. */
    protected static final int IGFS_BLOCK_SIZE = 1024;

    /** IGFS group size. */
    protected static final int IGFS_GROUP_SIZE = 32;

    /** Metadata cache name. */
    private static final String META_CACHE_NAME = "meta";

    /** File data cache name. */
    protected static final String DATA_CACHE_NAME = "data";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        cfg.setCacheConfiguration(metaConfiguration(), dataConfiguration());

        FileSystemConfiguration igfsCfg = new FileSystemConfiguration();

        igfsCfg.setName("igfs");
        igfsCfg.setMetaCacheName(META_CACHE_NAME);
        igfsCfg.setDataCacheName(DATA_CACHE_NAME);
        igfsCfg.setBlockSize(IGFS_BLOCK_SIZE);

        // Need to set this to avoid thread starvation.
        igfsCfg.setPerNodeParallelBatchCount(8);

        igfsCfg.setFragmentizerThrottlingBlockLength(16 * IGFS_BLOCK_SIZE);
        igfsCfg.setFragmentizerThrottlingDelay(10);

        cfg.setFileSystemConfiguration(igfsCfg);

        return cfg;
    }

    /**
     * Gets meta cache configuration.
     *
     * @return Meta cache configuration.
     */
    protected CacheConfiguration metaConfiguration() {
        CacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setName(META_CACHE_NAME);

        cfg.setCacheMode(REPLICATED);
        cfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cfg.setAtomicityMode(TRANSACTIONAL);

        return cfg;
    }

    /**
     * Gets data cache configuration.
     *
     * @return Data cache configuration.
     */
    protected CacheConfiguration dataConfiguration() {
        CacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setName(DATA_CACHE_NAME);

        cfg.setCacheMode(PARTITIONED);
        cfg.setBackups(0);
        cfg.setAffinityMapper(new IgfsGroupDataBlocksKeyMapper(IGFS_GROUP_SIZE));
        cfg.setNearConfiguration(null);
        cfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cfg.setAtomicityMode(TRANSACTIONAL);

        return cfg;
    }

    /**
     * @param gridIdx Grid index.
     * @param path Path to await.
     * @throws Exception If failed.
     */
    protected void awaitFileFragmenting(int gridIdx, IgfsPath path) throws Exception {
        IgfsEx igfs = (IgfsEx)grid(gridIdx).fileSystem("igfs");

        IgfsMetaManager meta = igfs.context().meta();

        IgniteUuid fileId = meta.fileId(path);

        if (fileId == null)
            throw new IgfsPathNotFoundException("File not found: " + path);

        IgfsFileInfo fileInfo = meta.info(fileId);

        do {
            if (fileInfo == null)
                throw new IgfsPathNotFoundException("File not found: " + path);

            if (fileInfo.fileMap().ranges().isEmpty())
                return;

            U.sleep(100);

            fileInfo = meta.info(fileId);
        }
        while (true);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(NODE_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        grid(0).fileSystem("igfs").format();
    }
}