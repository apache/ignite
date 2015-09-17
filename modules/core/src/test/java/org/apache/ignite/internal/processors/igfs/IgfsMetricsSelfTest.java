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

package org.apache.ignite.internal.processors.igfs;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.igfs.IgfsGroupDataBlocksKeyMapper;
import org.apache.ignite.igfs.IgfsInputStream;
import org.apache.ignite.igfs.IgfsIpcEndpointConfiguration;
import org.apache.ignite.igfs.IgfsIpcEndpointType;
import org.apache.ignite.igfs.IgfsMetrics;
import org.apache.ignite.igfs.IgfsMode;
import org.apache.ignite.igfs.IgfsOutputStream;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.igfs.IgfsMode.DUAL_SYNC;
import static org.apache.ignite.igfs.IgfsMode.PRIMARY;

/**
 * Test for IGFS metrics.
 */
public class IgfsMetricsSelfTest extends IgfsCommonAbstractTest {
    /** Primary IGFS name. */
    private static final String IGFS_PRIMARY = "igfs-primary";

    /** Primary IGFS name. */
    private static final String IGFS_SECONDARY = "igfs-secondary";

    /** Secondary file system REST endpoint configuration map. */
    private static final IgfsIpcEndpointConfiguration SECONDARY_REST_CFG;

    /** Test nodes count. */
    private static final int NODES_CNT = 3;

    /** IP finder for the grid with the primary file system. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Primary IGFS instances. */
    private static IgniteFileSystem[] igfsPrimary;

    /** Secondary IGFS instance. */
    private static IgfsImpl igfsSecondary;

    /** Primary file system block size. */
    public static final int PRIMARY_BLOCK_SIZE = 512;

    /** Secondary file system block size. */
    public static final int SECONDARY_BLOCK_SIZE = 512;

    static {
        SECONDARY_REST_CFG = new IgfsIpcEndpointConfiguration();

        SECONDARY_REST_CFG.setType(IgfsIpcEndpointType.TCP);
        SECONDARY_REST_CFG.setPort(11500);
    }

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
        igfsPrimary = new IgniteFileSystem[NODES_CNT];

        for (int i = 0; i < NODES_CNT; i++) {
            Ignite g = G.start(primaryConfiguration(i));

            igfsPrimary[i] = g.fileSystem(IGFS_PRIMARY);
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
        FileSystemConfiguration igfsCfg = new FileSystemConfiguration();

        igfsCfg.setDataCacheName("dataCache");
        igfsCfg.setMetaCacheName("metaCache");
        igfsCfg.setName(IGFS_PRIMARY);
        igfsCfg.setBlockSize(PRIMARY_BLOCK_SIZE);
        igfsCfg.setDefaultMode(PRIMARY);
        igfsCfg.setSecondaryFileSystem(igfsSecondary.asSecondary());

        Map<String, IgfsMode> pathModes = new HashMap<>();

        pathModes.put("/fileRemote", DUAL_SYNC);

        igfsCfg.setPathModes(pathModes);

        CacheConfiguration dataCacheCfg = defaultCacheConfiguration();

        dataCacheCfg.setName("dataCache");
        dataCacheCfg.setCacheMode(PARTITIONED);
        dataCacheCfg.setNearConfiguration(null);
        dataCacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        dataCacheCfg.setAffinityMapper(new IgfsGroupDataBlocksKeyMapper(128));
        dataCacheCfg.setBackups(0);
        dataCacheCfg.setAtomicityMode(TRANSACTIONAL);

        CacheConfiguration metaCacheCfg = defaultCacheConfiguration();

        metaCacheCfg.setName("metaCache");
        metaCacheCfg.setCacheMode(REPLICATED);
        metaCacheCfg.setNearConfiguration(null);
        metaCacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        metaCacheCfg.setAtomicityMode(TRANSACTIONAL);

        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setGridName("grid-" + idx);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);
        cfg.setCacheConfiguration(dataCacheCfg, metaCacheCfg);
        cfg.setFileSystemConfiguration(igfsCfg);

        cfg.setLocalHost("127.0.0.1");

        return cfg;
    }

    /**
     * Start a grid with the secondary file system.
     *
     * @throws Exception If failed.
     */
    private void startSecondary() throws Exception {
        FileSystemConfiguration igfsCfg = new FileSystemConfiguration();

        igfsCfg.setDataCacheName("dataCache");
        igfsCfg.setMetaCacheName("metaCache");
        igfsCfg.setName(IGFS_SECONDARY);
        igfsCfg.setBlockSize(SECONDARY_BLOCK_SIZE);
        igfsCfg.setDefaultMode(PRIMARY);
        igfsCfg.setIpcEndpointConfiguration(SECONDARY_REST_CFG);

        CacheConfiguration dataCacheCfg = defaultCacheConfiguration();

        dataCacheCfg.setName("dataCache");
        dataCacheCfg.setCacheMode(PARTITIONED);
        dataCacheCfg.setNearConfiguration(null);
        dataCacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        dataCacheCfg.setAffinityMapper(new IgfsGroupDataBlocksKeyMapper(128));
        dataCacheCfg.setBackups(0);
        dataCacheCfg.setAtomicityMode(TRANSACTIONAL);

        CacheConfiguration metaCacheCfg = defaultCacheConfiguration();

        metaCacheCfg.setName("metaCache");
        metaCacheCfg.setCacheMode(REPLICATED);
        metaCacheCfg.setNearConfiguration(null);
        metaCacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        metaCacheCfg.setAtomicityMode(TRANSACTIONAL);

        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setGridName("grid-secondary");

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(new TcpDiscoveryVmIpFinder(true));

        cfg.setDiscoverySpi(discoSpi);
        cfg.setCacheConfiguration(dataCacheCfg, metaCacheCfg);
        cfg.setFileSystemConfiguration(igfsCfg);

        cfg.setLocalHost("127.0.0.1");

        Ignite g = G.start(cfg);

        igfsSecondary = (IgfsImpl)g.fileSystem(IGFS_SECONDARY);
    }

    /** @throws Exception If failed. */
    public void testMetrics() throws Exception {
        IgniteFileSystem fs = igfsPrimary[0];

        assertNotNull(fs);

        IgfsMetrics m = fs.metrics();

        assertNotNull(m);
        assertEquals(0, m.directoriesCount());
        assertEquals(0, m.filesCount());
        assertEquals(0, m.filesOpenedForRead());
        assertEquals(0, m.filesOpenedForWrite());

        fs.mkdirs(new IgfsPath("/dir1"));

        m = fs.metrics();

        assertNotNull(m);
        assertEquals(1, m.directoriesCount());
        assertEquals(0, m.filesCount());
        assertEquals(0, m.filesOpenedForRead());
        assertEquals(0, m.filesOpenedForWrite());

        fs.mkdirs(new IgfsPath("/dir1/dir2/dir3"));
        fs.mkdirs(new IgfsPath("/dir4"));

        m = fs.metrics();

        assertNotNull(m);
        assertEquals(4, m.directoriesCount());
        assertEquals(0, m.filesCount());
        assertEquals(0, m.filesOpenedForRead());
        assertEquals(0, m.filesOpenedForWrite());

        IgfsOutputStream out1 = fs.create(new IgfsPath("/dir1/file1"), false);
        IgfsOutputStream out2 = fs.create(new IgfsPath("/dir1/file2"), false);
        IgfsOutputStream out3 = fs.create(new IgfsPath("/dir1/dir2/file"), false);

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

        IgfsOutputStream out = fs.append(new IgfsPath("/dir1/file1"), false);

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

        IgfsInputStream in1 = fs.open(new IgfsPath("/dir1/file1"));
        IgfsInputStream in2 = fs.open(new IgfsPath("/dir1/file2"));

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

        fs.delete(new IgfsPath("/dir1/file1"), false);
        fs.delete(new IgfsPath("/dir1/dir2"), true);

        m = fs.metrics();

        assertNotNull(m);
        assertEquals(2, m.directoriesCount());
        assertEquals(1, m.filesCount());
        assertEquals(0, m.filesOpenedForRead());
        assertEquals(0, m.filesOpenedForWrite());

        fs.delete(new IgfsPath("/"), true);

        m = fs.metrics();

        assertNotNull(m);
        assertEquals(0, m.directoriesCount());
        assertEquals(0, m.filesCount());
        assertEquals(0, m.filesOpenedForRead());
        assertEquals(0, m.filesOpenedForWrite());
    }

    /** @throws Exception If failed. */
    public void testMultipleClose() throws Exception {
        IgniteFileSystem fs = igfsPrimary[0];

        IgfsOutputStream out = fs.create(new IgfsPath("/file"), false);

        out.close();
        out.close();

        IgfsInputStream in = fs.open(new IgfsPath("/file"));

        in.close();
        in.close();

        IgfsMetrics m = fs.metrics();

        assertEquals(0, m.filesOpenedForWrite());
        assertEquals(0, m.filesOpenedForRead());
    }

    /**
     * Test block metrics.
     *
     * @throws Exception If failed.
     */
    public void testBlockMetrics() throws Exception {
        IgfsEx igfs = (IgfsEx)igfsPrimary[0];

        IgfsPath fileRemote = new IgfsPath("/fileRemote");
        IgfsPath file1 = new IgfsPath("/file1");
        IgfsPath file2 = new IgfsPath("/file2");

        // Create remote file and write some data to it.
        IgfsOutputStream out = igfsSecondary.create(fileRemote, 256, true, null, 1, 256, null);

        int rmtBlockSize = igfsSecondary.info(fileRemote).blockSize();

        out.write(new byte[rmtBlockSize]);
        out.close();

        // Start metrics measuring.
        IgfsMetrics initMetrics = igfs.metrics();

        // Create empty file.
        igfs.create(file1, 256, true, null, 1, 256, null).close();

        int blockSize = igfs.info(file1).blockSize();

        checkBlockMetrics(initMetrics, igfs.metrics(), 0, 0, 0, 0, 0, 0);

        // Write two blocks to the file.
        IgfsOutputStream os = igfs.append(file1, false);
        os.write(new byte[blockSize * 2]);
        os.close();

        checkBlockMetrics(initMetrics, igfs.metrics(), 0, 0, 0, 2, 0, blockSize * 2);

        // Write one more file (one block).
        os = igfs.create(file2, 256, true, null, 1, 256, null);
        os.write(new byte[blockSize]);
        os.close();

        checkBlockMetrics(initMetrics, igfs.metrics(), 0, 0, 0, 3, 0, blockSize * 3);

        // Read data from the first file.
        IgfsInputStreamAdapter is = igfs.open(file1);
        is.readFully(0, new byte[blockSize * 2]);
        is.close();

        checkBlockMetrics(initMetrics, igfs.metrics(), 2, 0, blockSize * 2, 3, 0, blockSize * 3);

        // Read data from the second file with hits.
        is = igfs.open(file2);
        is.readChunks(0, blockSize);
        is.close();

        checkBlockMetrics(initMetrics, igfs.metrics(), 3, 0, blockSize * 3, 3, 0, blockSize * 3);

        // Clear the first file.
        igfs.create(file1, true).close();

        checkBlockMetrics(initMetrics, igfs.metrics(), 3, 0, blockSize * 3, 3, 0, blockSize * 3);

        // Delete the second file.
        igfs.delete(file2, false);

        checkBlockMetrics(initMetrics, igfs.metrics(), 3, 0, blockSize * 3, 3, 0, blockSize * 3);

        // Read remote file.
        is = igfs.open(fileRemote);
        is.readChunks(0, rmtBlockSize);
        is.close();

        checkBlockMetrics(initMetrics, igfs.metrics(), 4, 1, blockSize * 3 + rmtBlockSize, 3, 0, blockSize * 3);

        // Lets wait for blocks will be placed to cache
        U.sleep(300);

        // Read remote file again.
        is = igfs.open(fileRemote);
        is.readChunks(0, rmtBlockSize);
        is.close();

        checkBlockMetrics(initMetrics, igfs.metrics(), 5, 1, blockSize * 3 + rmtBlockSize * 2, 3, 0, blockSize * 3);

        IgfsMetrics metrics = igfs.metrics();

        assert metrics.secondarySpaceSize() == rmtBlockSize;

        // Write some data to the file working in DUAL mode.
        os = igfs.append(fileRemote, false);
        os.write(new byte[rmtBlockSize]);
        os.close();

        // Additional block read here due to file ending synchronization.
        checkBlockMetrics(initMetrics, igfs.metrics(), 5, 1, blockSize * 3 + rmtBlockSize * 2, 4, 1,
            blockSize * 3 + rmtBlockSize);

        metrics = igfs.metrics();

        assert metrics.secondarySpaceSize() == rmtBlockSize * 2;

        igfs.delete(fileRemote, false);

        U.sleep(300);

        assert igfs.metrics().secondarySpaceSize() == 0;

        // Write partial block to the first file.
        os = igfs.append(file1, false);
        os.write(new byte[blockSize / 2]);
        os.close();

        checkBlockMetrics(initMetrics, igfs.metrics(), 5, 1, blockSize * 3 + rmtBlockSize * 2, 5, 1,
            blockSize * 7 / 2 + rmtBlockSize);

        // Now read partial block.
        // Read remote file again.
        is = igfs.open(file1);
        is.seek(blockSize * 2);
        is.readChunks(0, blockSize / 2);
        is.close();

        checkBlockMetrics(initMetrics, igfs.metrics(), 6, 1, blockSize * 7 / 2 + rmtBlockSize * 2, 5, 1,
            blockSize * 7 / 2 + rmtBlockSize);

        igfs.resetMetrics();

        metrics = igfs.metrics();

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
    private void checkBlockMetrics(IgfsMetrics initMetrics, IgfsMetrics metrics, long blocksRead,
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