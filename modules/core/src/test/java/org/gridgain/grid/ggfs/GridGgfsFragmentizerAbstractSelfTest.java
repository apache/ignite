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

import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.fs.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import static org.apache.ignite.cache.GridCacheAtomicityMode.*;
import static org.apache.ignite.cache.GridCacheMode.*;

/**
 * Fragmentizer abstract self test.
 */
public class GridGgfsFragmentizerAbstractSelfTest extends GridGgfsCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Test nodes count. */
    protected static final int NODE_CNT = 4;

    /** GGFS block size. */
    protected static final int GGFS_BLOCK_SIZE = 1024;

    /** GGFS group size. */
    protected static final int GGFS_GROUP_SIZE = 32;

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

        IgniteFsConfiguration ggfsCfg = new IgniteFsConfiguration();

        ggfsCfg.setName("ggfs");
        ggfsCfg.setMetaCacheName(META_CACHE_NAME);
        ggfsCfg.setDataCacheName(DATA_CACHE_NAME);
        ggfsCfg.setBlockSize(GGFS_BLOCK_SIZE);

        // Need to set this to avoid thread starvation.
        ggfsCfg.setPerNodeParallelBatchCount(8);

        ggfsCfg.setFragmentizerThrottlingBlockLength(16 * GGFS_BLOCK_SIZE);
        ggfsCfg.setFragmentizerThrottlingDelay(10);

        cfg.setGgfsConfiguration(ggfsCfg);

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
        cfg.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);
        cfg.setQueryIndexEnabled(false);
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
        cfg.setAffinityMapper(new IgniteFsGroupDataBlocksKeyMapper(GGFS_GROUP_SIZE));
        cfg.setDistributionMode(GridCacheDistributionMode.PARTITIONED_ONLY);
        cfg.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);
        cfg.setQueryIndexEnabled(false);
        cfg.setAtomicityMode(TRANSACTIONAL);

        return cfg;
    }

    /**
     * @param gridIdx Grid index.
     * @param path Path to await.
     * @throws Exception If failed.
     */
    protected void awaitFileFragmenting(int gridIdx, IgniteFsPath path) throws Exception {
        GridGgfsEx ggfs = (GridGgfsEx)grid(gridIdx).fileSystem("ggfs");

        GridGgfsMetaManager meta = ggfs.context().meta();

        IgniteUuid fileId = meta.fileId(path);

        if (fileId == null)
            throw new IgniteFsFileNotFoundException("File not found: " + path);

        GridGgfsFileInfo fileInfo = meta.info(fileId);

        do {
            if (fileInfo == null)
                throw new IgniteFsFileNotFoundException("File not found: " + path);

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
        grid(0).fileSystem("ggfs").format();
    }
}
