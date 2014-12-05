/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.ggfs;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.fs.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.eviction.ggfs.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;

import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.apache.ignite.fs.IgniteFsMode.*;

/**
 * Tests for GGFS per-block LR eviction policy.
 */
@SuppressWarnings({"ConstantConditions", "ThrowableResultOfMethodCallIgnored"})
public class GridCacheGgfsPerBlockLruEvictionPolicySelfTest extends GridGgfsCommonAbstractTest {
    /** Primary GGFS name. */
    private static final String GGFS_PRIMARY = "ggfs-primary";

    /** Primary GGFS name. */
    private static final String GGFS_SECONDARY = "ggfs-secondary";

    /** Secondary file system URI. */
    private static final String SECONDARY_URI = "ggfs://ggfs-secondary:grid-secondary@127.0.0.1:11500/";

    /** Secondary file system configuration path. */
    private static final String SECONDARY_CFG = "modules/core/src/test/config/hadoop/core-site-loopback-secondary.xml";

    /** Secondary file system REST endpoint configuration string. */
    private static final String SECONDARY_REST_CFG = "{type:'tcp', port:11500}";

    /** File working in PRIMARY mode. */
    public static final IgniteFsPath FILE = new IgniteFsPath("/file");

    /** File working in DUAL mode. */
    public static final IgniteFsPath FILE_RMT = new IgniteFsPath("/fileRemote");

    /** Primary GGFS instances. */
    private static GridGgfsImpl ggfsPrimary;

    /** Secondary GGFS instance. */
    private static IgniteFs secondaryFs;

    /** Primary file system data cache. */
    private static GridCacheAdapter<GridGgfsBlockKey, byte[]> dataCache;

    /** Eviction policy */
    private static GridCacheGgfsPerBlockLruEvictionPolicy evictPlc;

    /**
     * Start a grid with the primary file system.
     *
     * @throws Exception If failed.
     */
    private void startPrimary() throws Exception {
        IgniteFsConfiguration ggfsCfg = new IgniteFsConfiguration();

        ggfsCfg.setDataCacheName("dataCache");
        ggfsCfg.setMetaCacheName("metaCache");
        ggfsCfg.setName(GGFS_PRIMARY);
        ggfsCfg.setBlockSize(512);
        ggfsCfg.setDefaultMode(PRIMARY);
        ggfsCfg.setPrefetchBlocks(1);
        ggfsCfg.setSequentialReadsBeforePrefetch(Integer.MAX_VALUE);
        ggfsCfg.setSecondaryFileSystem(secondaryFs);

        Map<String, IgniteFsMode> pathModes = new HashMap<>();

        pathModes.put(FILE_RMT.toString(), DUAL_SYNC);

        ggfsCfg.setPathModes(pathModes);

        GridCacheConfiguration dataCacheCfg = defaultCacheConfiguration();

        dataCacheCfg.setName("dataCache");
        dataCacheCfg.setCacheMode(PARTITIONED);
        dataCacheCfg.setDistributionMode(GridCacheDistributionMode.PARTITIONED_ONLY);
        dataCacheCfg.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);
        dataCacheCfg.setAtomicityMode(TRANSACTIONAL);

        evictPlc = new GridCacheGgfsPerBlockLruEvictionPolicy();

        dataCacheCfg.setEvictionPolicy(evictPlc);
        dataCacheCfg.setAffinityMapper(new IgniteFsGroupDataBlocksKeyMapper(128));
        dataCacheCfg.setBackups(0);
        dataCacheCfg.setQueryIndexEnabled(false);

        GridCacheConfiguration metaCacheCfg = defaultCacheConfiguration();

        metaCacheCfg.setName("metaCache");
        metaCacheCfg.setCacheMode(REPLICATED);
        metaCacheCfg.setDistributionMode(GridCacheDistributionMode.PARTITIONED_ONLY);
        metaCacheCfg.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);
        metaCacheCfg.setQueryIndexEnabled(false);
        metaCacheCfg.setAtomicityMode(TRANSACTIONAL);

        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setGridName("grid-primary");

        GridTcpDiscoverySpi discoSpi = new GridTcpDiscoverySpi();

        discoSpi.setIpFinder(new GridTcpDiscoveryVmIpFinder(true));

        cfg.setDiscoverySpi(discoSpi);
        cfg.setCacheConfiguration(dataCacheCfg, metaCacheCfg);
        cfg.setGgfsConfiguration(ggfsCfg);

        cfg.setLocalHost("127.0.0.1");
        cfg.setRestEnabled(false);

        Ignite g = G.start(cfg);

        ggfsPrimary = (GridGgfsImpl)g.fileSystem(GGFS_PRIMARY);

        dataCache = ggfsPrimary.context().kernalContext().cache().internalCache(
            ggfsPrimary.context().configuration().getDataCacheName());
    }

    /**
     * Start a grid with the secondary file system.
     *
     * @throws Exception If failed.
     */
    private void startSecondary() throws Exception {
        IgniteFsConfiguration ggfsCfg = new IgniteFsConfiguration();

        ggfsCfg.setDataCacheName("dataCache");
        ggfsCfg.setMetaCacheName("metaCache");
        ggfsCfg.setName(GGFS_SECONDARY);
        ggfsCfg.setBlockSize(512);
        ggfsCfg.setDefaultMode(PRIMARY);
        ggfsCfg.setIpcEndpointConfiguration(GridGgfsTestUtils.jsonToMap(SECONDARY_REST_CFG));

        GridCacheConfiguration dataCacheCfg = defaultCacheConfiguration();

        dataCacheCfg.setName("dataCache");
        dataCacheCfg.setCacheMode(PARTITIONED);
        dataCacheCfg.setDistributionMode(GridCacheDistributionMode.PARTITIONED_ONLY);
        dataCacheCfg.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);
        dataCacheCfg.setAffinityMapper(new IgniteFsGroupDataBlocksKeyMapper(128));
        dataCacheCfg.setBackups(0);
        dataCacheCfg.setQueryIndexEnabled(false);
        dataCacheCfg.setAtomicityMode(TRANSACTIONAL);

        GridCacheConfiguration metaCacheCfg = defaultCacheConfiguration();

        metaCacheCfg.setName("metaCache");
        metaCacheCfg.setCacheMode(REPLICATED);
        metaCacheCfg.setDistributionMode(GridCacheDistributionMode.PARTITIONED_ONLY);
        metaCacheCfg.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);
        metaCacheCfg.setQueryIndexEnabled(false);
        metaCacheCfg.setAtomicityMode(TRANSACTIONAL);

        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setGridName("grid-secondary");

        GridTcpDiscoverySpi discoSpi = new GridTcpDiscoverySpi();

        discoSpi.setIpFinder(new GridTcpDiscoveryVmIpFinder(true));

        cfg.setDiscoverySpi(discoSpi);
        cfg.setCacheConfiguration(dataCacheCfg, metaCacheCfg);
        cfg.setGgfsConfiguration(ggfsCfg);

        cfg.setLocalHost("127.0.0.1");
        cfg.setRestEnabled(false);

        Ignite g = G.start(cfg);

        secondaryFs = g.fileSystem(GGFS_SECONDARY);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        try {
            // Cleanup.
            ggfsPrimary.format();

            while (!dataCache.isEmpty())
                U.sleep(100);

            checkEvictionPolicy(0, 0);
        }
        finally {
            stopAllGrids(false);
        }
    }

    /**
     * Startup primary and secondary file systems.
     *
     * @throws Exception If failed.
     */
    private void start() throws Exception {
        startSecondary();
        startPrimary();

        evictPlc.setMaxBlocks(0);
        evictPlc.setMaxSize(0);
        evictPlc.setExcludePaths(null);
    }

    /**
     * Test how evictions are handled for a file working in PRIMARY mode.
     *
     * @throws Exception If failed.
     */
    public void testFilePrimary() throws Exception {
        start();

        // Create file in primary mode. It must not be propagated to eviction policy.
        ggfsPrimary.create(FILE, true).close();

        checkEvictionPolicy(0, 0);

        int blockSize = ggfsPrimary.info(FILE).blockSize();

        append(FILE, blockSize);

        checkEvictionPolicy(0, 0);

        read(FILE, 0, blockSize);

        checkEvictionPolicy(0, 0);
    }

    /**
     * Test how evictions are handled for a file working in PRIMARY mode.
     *
     * @throws Exception If failed.
     */
    public void testFileDual() throws Exception {
        start();

        ggfsPrimary.create(FILE_RMT, true).close();

        checkEvictionPolicy(0, 0);

        int blockSize = ggfsPrimary.info(FILE_RMT).blockSize();

        // File write.
        append(FILE_RMT, blockSize);

        checkEvictionPolicy(1, blockSize);

        // One more write.
        append(FILE_RMT, blockSize);

        checkEvictionPolicy(2, blockSize * 2);

        // Read.
        read(FILE_RMT, 0, blockSize);

        checkEvictionPolicy(2, blockSize * 2);
    }

    /**
     * Ensure that a DUAL mode file is not propagated to eviction policy
     *
     * @throws Exception If failed.
     */
    public void testFileDualExclusion() throws Exception {
        start();

        evictPlc.setExcludePaths(Collections.singleton(FILE_RMT.toString()));

        // Create file in primary mode. It must not be propagated to eviction policy.
        ggfsPrimary.create(FILE_RMT, true).close();

        checkEvictionPolicy(0, 0);

        int blockSize = ggfsPrimary.info(FILE_RMT).blockSize();

        append(FILE_RMT, blockSize);

        checkEvictionPolicy(0, 0);

        read(FILE_RMT, 0, blockSize);

        checkEvictionPolicy(0, 0);
    }

    /**
     * Ensure that exception is thrown in case we are trying to rename file with one exclude setting to the file with
     * another.
     *
     * @throws Exception If failed.
     */
    public void testRenameDifferentExcludeSettings() throws Exception {
        start();

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                ggfsPrimary.rename(FILE, FILE_RMT);

                return null;
            }
        }, IgniteFsInvalidPathException.class, "Cannot move file to a path with different eviction exclude setting " +
            "(need to copy and remove)");

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                ggfsPrimary.rename(FILE_RMT, FILE);

                return null;
            }
        }, IgniteFsInvalidPathException.class, "Cannot move file to a path with different eviction exclude setting " +
            "(need to copy and remove)");
    }

    /**
     * Test eviction caused by too much blocks.
     *
     * @throws Exception If failed.
     */
    public void testBlockCountEviction() throws Exception {
        start();

        int blockCnt = 3;

        evictPlc.setMaxBlocks(blockCnt);

        ggfsPrimary.create(FILE_RMT, true).close();

        checkEvictionPolicy(0, 0);

        int blockSize = ggfsPrimary.info(FILE_RMT).blockSize();

        // Write blocks up to the limit.
        append(FILE_RMT, blockSize * blockCnt);

        checkEvictionPolicy(blockCnt, blockCnt * blockSize);

        // Write one more block what should cause eviction.
        append(FILE_RMT, blockSize);

        checkEvictionPolicy(blockCnt, blockCnt * blockSize);

        // Read the first block.
        read(FILE_RMT, 0, blockSize);

        checkEvictionPolicy(blockCnt, blockCnt * blockSize);
        checkMetrics(1, 1);
    }

    /**
     * Test eviction caused by too big data size.
     *
     * @throws Exception If failed.
     */
    public void testDataSizeEviction() throws Exception {
        start();

        ggfsPrimary.create(FILE_RMT, true).close();

        int blockCnt = 3;
        int blockSize = ggfsPrimary.info(FILE_RMT).blockSize();

        evictPlc.setMaxSize(blockSize * blockCnt);

        // Write blocks up to the limit.
        append(FILE_RMT, blockSize * blockCnt);

        checkEvictionPolicy(blockCnt, blockCnt * blockSize);

        // Reset metrics.
        ggfsPrimary.resetMetrics();

        // Read the first block what should cause reordering.
        read(FILE_RMT, 0, blockSize);

        checkMetrics(1, 0);
        checkEvictionPolicy(blockCnt, blockCnt * blockSize);

        // Write one more block what should cause eviction of the block 2.
        append(FILE_RMT, blockSize);

        checkEvictionPolicy(blockCnt, blockCnt * blockSize);

        // Read the first block.
        read(FILE_RMT, 0, blockSize);

        checkMetrics(2, 0);
        checkEvictionPolicy(blockCnt, blockCnt * blockSize);

        // Read the second block (which was evicted).
        read(FILE_RMT, blockSize, blockSize);

        checkMetrics(3, 1);
        checkEvictionPolicy(blockCnt, blockCnt * blockSize);
    }

    /**
     * Read some data from the given file with the given offset.
     *
     * @param path File path.
     * @param off Offset.
     * @param len Length.
     * @throws Exception If failed.
     */
    private void read(IgniteFsPath path, int off, int len) throws Exception {
        IgniteFsInputStream is = ggfsPrimary.open(path);

        is.readFully(off, new byte[len]);

        is.close();
    }

    /**
     * Append some data to the given file.
     *
     * @param path File path.
     * @param len Data length.
     * @throws Exception If failed.
     */
    private void append(IgniteFsPath path, int len) throws Exception {
        IgniteFsOutputStream os = ggfsPrimary.append(path, false);

        os.write(new byte[len]);

        os.close();
    }

    /**
     * Check metrics counters.
     *
     * @param blocksRead Expected blocks read.
     * @param blocksReadRmt Expected blocks read remote.
     * @throws Exception If failed.
     */
    public void checkMetrics(final long blocksRead, final long blocksReadRmt) throws Exception {
        assert GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                try {
                    IgniteFsMetrics metrics = ggfsPrimary.metrics();

                    return metrics.blocksReadTotal() == blocksRead && metrics.blocksReadRemote() == blocksReadRmt;
                }
                catch (GridException e) {
                    throw new RuntimeException(e);
                }
            }
        }, 5000) : "Unexpected metrics [expectedBlocksReadTotal=" + blocksRead + ", actualBlocksReadTotal=" +
            ggfsPrimary.metrics().blocksReadTotal() + ", expectedBlocksReadRemote=" + blocksReadRmt +
            ", actualBlocksReadRemote=" + ggfsPrimary.metrics().blocksReadRemote() + ']';
    }

    /**
     * Check eviction policy state.
     *
     * @param curBlocks Current blocks.
     * @param curBytes Current bytes.
     */
    private void checkEvictionPolicy(final int curBlocks, final long curBytes) throws GridInterruptedException {
        assert GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return evictPlc.getCurrentBlocks() == curBlocks && evictPlc.getCurrentSize() == curBytes;
            }
        }, 5000) : "Unexpected counts [expectedBlocks=" + curBlocks + ", actualBlocks=" + evictPlc.getCurrentBlocks() +
            ", expectedBytes=" + curBytes + ", currentBytes=" + curBytes + ']';
    }
}
