/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.ggfs;

import org.apache.ignite.configuration.*;
import org.apache.ignite.fs.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.ggfs.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.internal.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;

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
    protected GridCacheConfiguration metaConfiguration() {
        GridCacheConfiguration cfg = defaultCacheConfiguration();

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
    protected GridCacheConfiguration dataConfiguration() {
        GridCacheConfiguration cfg = defaultCacheConfiguration();

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
