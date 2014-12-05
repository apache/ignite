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
import org.gridgain.grid.cache.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.U;
import org.gridgain.testframework.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.ggfs.GridGgfsMode.*;

/**
 * Test for GGFS metrics.
 */
public class GridGgfsMetricsSelfTest extends GridGgfsCommonAbstractTest {
    /** Primary GGFS name. */
    private static final String GGFS_PRIMARY = "ggfs-primary";

    /** Primary GGFS name. */
    private static final String GGFS_SECONDARY = "ggfs-secondary";

    /** Secondary file system REST endpoint configuration string. */
    private static final String SECONDARY_REST_CFG = "{type:'tcp', port:11500}";

    /** Test nodes count. */
    private static final int NODES_CNT = 3;

    /** IP finder for the grid with the primary file system. */
    private static final GridTcpDiscoveryIpFinder IP_FINDER = new GridTcpDiscoveryVmIpFinder(true);

    /** Primary GGFS instances. */
    private static IgniteFs[] ggfsPrimary;

    /** Secondary GGFS instance. */
    private static IgniteFs ggfsSecondary;

    /** Primary file system block size. */
    public static final int PRIMARY_BLOCK_SIZE = 512;

    /** Secondary file system block size. */
    public static final int SECONDARY_BLOCK_SIZE = 512;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startSecondary();
        startPrimary();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids(false);
    }

    /**
     * Start a grid with the primary file system.
     *
     * @throws Exception If failed.
     */
    private void startPrimary() throws Exception {
        ggfsPrimary = new IgniteFs[NODES_CNT];

        for (int i = 0; i < NODES_CNT; i++) {
            Ignite g = G.start(primaryConfiguration(i));

            ggfsPrimary[i] = g.fileSystem(GGFS_PRIMARY);
        }
    }

    /**
     * Get configuration for a grid with the primary file system.
     *
     * @param idx Node index.
     * @return Configuration.
     * @throws Exception If failed.
     */
    private IgniteConfiguration primaryConfiguration(int idx) throws Exception {
        IgniteFsConfiguration ggfsCfg = new IgniteFsConfiguration();

        ggfsCfg.setDataCacheName("dataCache");
        ggfsCfg.setMetaCacheName("metaCache");
        ggfsCfg.setName(GGFS_PRIMARY);
        ggfsCfg.setBlockSize(PRIMARY_BLOCK_SIZE);
        ggfsCfg.setDefaultMode(PRIMARY);
        ggfsCfg.setSecondaryFileSystem(ggfsSecondary);

        Map<String, GridGgfsMode> pathModes = new HashMap<>();

        pathModes.put("/fileRemote", DUAL_SYNC);

        ggfsCfg.setPathModes(pathModes);

        GridCacheConfiguration dataCacheCfg = defaultCacheConfiguration();

        dataCacheCfg.setName("dataCache");
        dataCacheCfg.setCacheMode(PARTITIONED);
        dataCacheCfg.setDistributionMode(GridCacheDistributionMode.PARTITIONED_ONLY);
        dataCacheCfg.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);
        dataCacheCfg.setAffinityMapper(new GridGgfsGroupDataBlocksKeyMapper(128));
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

        cfg.setGridName("grid-" + idx);

        GridTcpDiscoverySpi discoSpi = new GridTcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);
        cfg.setCacheConfiguration(dataCacheCfg, metaCacheCfg);
        cfg.setGgfsConfiguration(ggfsCfg);

        cfg.setLocalHost("127.0.0.1");

        return cfg;
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
        ggfsCfg.setBlockSize(SECONDARY_BLOCK_SIZE);
        ggfsCfg.setDefaultMode(PRIMARY);
        ggfsCfg.setIpcEndpointConfiguration(GridGgfsTestUtils.jsonToMap(SECONDARY_REST_CFG));

        GridCacheConfiguration dataCacheCfg = defaultCacheConfiguration();

        dataCacheCfg.setName("dataCache");
        dataCacheCfg.setCacheMode(PARTITIONED);
        dataCacheCfg.setDistributionMode(GridCacheDistributionMode.PARTITIONED_ONLY);
        dataCacheCfg.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);
        dataCacheCfg.setAffinityMapper(new GridGgfsGroupDataBlocksKeyMapper(128));
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

        Ignite g = G.start(cfg);

        ggfsSecondary = g.fileSystem(GGFS_SECONDARY);
    }

    /** @throws Exception If failed. */
    public void testMetrics() throws Exception {
        IgniteFs fs = ggfsPrimary[0];

        assertNotNull(fs);

        GridGgfsMetrics m = fs.metrics();

        assertNotNull(m);
        assertEquals(0, m.directoriesCount());
        assertEquals(0, m.filesCount());
        assertEquals(0, m.filesOpenedForRead());
        assertEquals(0, m.filesOpenedForWrite());

        fs.mkdirs(new IgniteFsPath("/dir1"));

        m = fs.metrics();

        assertNotNull(m);
        assertEquals(1, m.directoriesCount());
        assertEquals(0, m.filesCount());
        assertEquals(0, m.filesOpenedForRead());
        assertEquals(0, m.filesOpenedForWrite());

        fs.mkdirs(new IgniteFsPath("/dir1/dir2/dir3"));
        fs.mkdirs(new IgniteFsPath("/dir4"));

        m = fs.metrics();

        assertNotNull(m);
        assertEquals(4, m.directoriesCount());
        assertEquals(0, m.filesCount());
        assertEquals(0, m.filesOpenedForRead());
        assertEquals(0, m.filesOpenedForWrite());

        GridGgfsOutputStream out1 = fs.create(new IgniteFsPath("/dir1/file1"), false);
        GridGgfsOutputStream out2 = fs.create(new IgniteFsPath("/dir1/file2"), false);
        GridGgfsOutputStream out3 = fs.create(new IgniteFsPath("/dir1/dir2/file"), false);

        m = fs.metrics();

        assertNotNull(m);
        assertEquals(4, m.directoriesCount());
        assertEquals(3, m.filesCount());
        assertEquals(0, m.filesOpenedForRead());
        assertEquals(3, m.filesOpenedForWrite());

        out1.write(new byte[10]);
        out2.write(new byte[20]);
        out3.write(new byte[30]);

        out1.close();

        m = fs.metrics();

        assertNotNull(m);
        assertEquals(4, m.directoriesCount());
        assertEquals(3, m.filesCount());
        assertEquals(0, m.filesOpenedForRead());
        assertEquals(2, m.filesOpenedForWrite());

        out2.close();
        out3.close();

        m = fs.metrics();

        assertNotNull(m);
        assertEquals(4, m.directoriesCount());
        assertEquals(3, m.filesCount());
        assertEquals(0, m.filesOpenedForRead());
        assertEquals(0, m.filesOpenedForWrite());

        GridGgfsOutputStream out = fs.append(new IgniteFsPath("/dir1/file1"), false);

        out.write(new byte[20]);

        m = fs.metrics();

        assertNotNull(m);
        assertEquals(4, m.directoriesCount());
        assertEquals(3, m.filesCount());
        assertEquals(0, m.filesOpenedForRead());
        assertEquals(1, m.filesOpenedForWrite());

        out.write(new byte[20]);

        out.close();

        m = fs.metrics();

        assertNotNull(m);
        assertEquals(4, m.directoriesCount());
        assertEquals(3, m.filesCount());
        assertEquals(0, m.filesOpenedForRead());
        assertEquals(0, m.filesOpenedForWrite());

        GridGgfsInputStream in1 = fs.open(new IgniteFsPath("/dir1/file1"));
        GridGgfsInputStream in2 = fs.open(new IgniteFsPath("/dir1/file2"));

        m = fs.metrics();

        assertNotNull(m);
        assertEquals(4, m.directoriesCount());
        assertEquals(3, m.filesCount());
        assertEquals(2, m.filesOpenedForRead());
        assertEquals(0, m.filesOpenedForWrite());

        in1.close();
        in2.close();

        m = fs.metrics();

        assertNotNull(m);
        assertEquals(4, m.directoriesCount());
        assertEquals(3, m.filesCount());
        assertEquals(0, m.filesOpenedForRead());
        assertEquals(0, m.filesOpenedForWrite());

        fs.delete(new IgniteFsPath("/dir1/file1"), false);
        fs.delete(new IgniteFsPath("/dir1/dir2"), true);

        m = fs.metrics();

        assertNotNull(m);
        assertEquals(2, m.directoriesCount());
        assertEquals(1, m.filesCount());
        assertEquals(0, m.filesOpenedForRead());
        assertEquals(0, m.filesOpenedForWrite());

        fs.delete(new IgniteFsPath("/"), true);

        m = fs.metrics();

        assertNotNull(m);
        assertEquals(0, m.directoriesCount());
        assertEquals(0, m.filesCount());
        assertEquals(0, m.filesOpenedForRead());
        assertEquals(0, m.filesOpenedForWrite());
    }

    /** @throws Exception If failed. */
    public void testMultipleClose() throws Exception {
        IgniteFs fs = ggfsPrimary[0];

        GridGgfsOutputStream out = fs.create(new IgniteFsPath("/file"), false);

        out.close();
        out.close();

        GridGgfsInputStream in = fs.open(new IgniteFsPath("/file"));

        in.close();
        in.close();

        GridGgfsMetrics m = fs.metrics();

        assertEquals(0, m.filesOpenedForWrite());
        assertEquals(0, m.filesOpenedForRead());
    }

    /**
     * Test block metrics.
     *
     * @throws Exception If failed.
     */
    public void testBlockMetrics() throws Exception {
        GridGgfsEx ggfs = (GridGgfsEx)ggfsPrimary[0];

        IgniteFsPath fileRemote = new IgniteFsPath("/fileRemote");
        IgniteFsPath file1 = new IgniteFsPath("/file1");
        IgniteFsPath file2 = new IgniteFsPath("/file2");

        // Create remote file and write some data to it.
        GridGgfsOutputStream out = ggfsSecondary.create(fileRemote, 256, true, null, 1, 256, null);

        int rmtBlockSize = ggfsSecondary.info(fileRemote).blockSize();

        out.write(new byte[rmtBlockSize]);
        out.close();

        // Start metrics measuring.
        GridGgfsMetrics initMetrics = ggfs.metrics();

        // Create empty file.
        ggfs.create(file1, 256, true, null, 1, 256, null).close();

        int blockSize = ggfs.info(file1).blockSize();

        checkBlockMetrics(initMetrics, ggfs.metrics(), 0, 0, 0, 0, 0, 0);

        // Write two blocks to the file.
        GridGgfsOutputStream os = ggfs.append(file1, false);
        os.write(new byte[blockSize * 2]);
        os.close();

        checkBlockMetrics(initMetrics, ggfs.metrics(), 0, 0, 0, 2, 0, blockSize * 2);

        // Write one more file (one block).
        os = ggfs.create(file2, 256, true, null, 1, 256, null);
        os.write(new byte[blockSize]);
        os.close();

        checkBlockMetrics(initMetrics, ggfs.metrics(), 0, 0, 0, 3, 0, blockSize * 3);

        // Read data from the first file.
        GridGgfsInputStreamAdapter is = ggfs.open(file1);
        is.readFully(0, new byte[blockSize * 2]);
        is.close();

        checkBlockMetrics(initMetrics, ggfs.metrics(), 2, 0, blockSize * 2, 3, 0, blockSize * 3);

        // Read data from the second file with hits.
        is = ggfs.open(file2);
        is.readChunks(0, blockSize);
        is.close();

        checkBlockMetrics(initMetrics, ggfs.metrics(), 3, 0, blockSize * 3, 3, 0, blockSize * 3);

        // Clear the first file.
        ggfs.create(file1, true).close();

        checkBlockMetrics(initMetrics, ggfs.metrics(), 3, 0, blockSize * 3, 3, 0, blockSize * 3);

        // Delete the second file.
        ggfs.delete(file2, false);

        checkBlockMetrics(initMetrics, ggfs.metrics(), 3, 0, blockSize * 3, 3, 0, blockSize * 3);

        // Read remote file.
        is = ggfs.open(fileRemote);
        is.readChunks(0, rmtBlockSize);
        is.close();

        checkBlockMetrics(initMetrics, ggfs.metrics(), 4, 1, blockSize * 3 + rmtBlockSize, 3, 0, blockSize * 3);

        // Lets wait for blocks will be placed to cache
        U.sleep(300);

        // Read remote file again.
        is = ggfs.open(fileRemote);
        is.readChunks(0, rmtBlockSize);
        is.close();

        checkBlockMetrics(initMetrics, ggfs.metrics(), 5, 1, blockSize * 3 + rmtBlockSize * 2, 3, 0, blockSize * 3);

        GridGgfsMetrics metrics = ggfs.metrics();

        assert metrics.secondarySpaceSize() == rmtBlockSize;

        // Write some data to the file working in DUAL mode.
        os = ggfs.append(fileRemote, false);
        os.write(new byte[rmtBlockSize]);
        os.close();

        // Additional block read here due to file ending synchronization.
        checkBlockMetrics(initMetrics, ggfs.metrics(), 5, 1, blockSize * 3 + rmtBlockSize * 2, 4, 1,
            blockSize * 3 + rmtBlockSize);

        metrics = ggfs.metrics();

        assert metrics.secondarySpaceSize() == rmtBlockSize * 2;

        ggfs.delete(fileRemote, false);

        U.sleep(300);

        assert ggfs.metrics().secondarySpaceSize() == 0;

        // Write partial block to the first file.
        os = ggfs.append(file1, false);
        os.write(new byte[blockSize / 2]);
        os.close();

        checkBlockMetrics(initMetrics, ggfs.metrics(), 5, 1, blockSize * 3 + rmtBlockSize * 2, 5, 1,
            blockSize * 7 / 2 + rmtBlockSize);

        // Now read partial block.
        // Read remote file again.
        is = ggfs.open(file1);
        is.seek(blockSize * 2);
        is.readChunks(0, blockSize / 2);
        is.close();

        checkBlockMetrics(initMetrics, ggfs.metrics(), 6, 1, blockSize * 7 / 2 + rmtBlockSize * 2, 5, 1,
            blockSize * 7 / 2 + rmtBlockSize);

        ggfs.resetMetrics();

        metrics = ggfs.metrics();

        assert metrics.blocksReadTotal() == 0;
        assert metrics.blocksReadRemote() == 0;
        assert metrics.blocksWrittenTotal() == 0;
        assert metrics.blocksWrittenRemote() == 0;
        assert metrics.bytesRead() == 0;
        assert metrics.bytesReadTime() == 0;
        assert metrics.bytesWritten() == 0;
        assert metrics.bytesWriteTime() == 0;
    }

    /**
     * Ensure overall block-related metrics correctness.
     *
     * @param initMetrics Initial metrics.
     * @param metrics Metrics to check.
     * @param blocksRead Blocks read remote.
     * @param blocksReadRemote Blocks read remote.
     * @param bytesRead Bytes read.
     * @param blocksWrite Blocks write.
     * @param blocksWriteRemote Blocks write remote.
     * @param bytesWrite Bytes write.
     * @throws Exception If failed.
     */
    private void checkBlockMetrics(GridGgfsMetrics initMetrics, GridGgfsMetrics metrics, long blocksRead,
        long blocksReadRemote, long bytesRead, long blocksWrite, long blocksWriteRemote, long bytesWrite)
        throws Exception {
        assert metrics != null;

        assertEquals(blocksRead, metrics.blocksReadTotal() - initMetrics.blocksReadTotal());
        assertEquals(blocksReadRemote, metrics.blocksReadRemote() - initMetrics.blocksReadRemote());
        assertEquals(bytesRead, metrics.bytesRead() - initMetrics.bytesRead());

        assertEquals(blocksWrite, metrics.blocksWrittenTotal() - initMetrics.blocksWrittenTotal());
        assertEquals(blocksWriteRemote, metrics.blocksWrittenRemote() - initMetrics.blocksWrittenRemote());
        assertEquals(bytesWrite, metrics.bytesWritten() - initMetrics.bytesWritten());
    }
}
