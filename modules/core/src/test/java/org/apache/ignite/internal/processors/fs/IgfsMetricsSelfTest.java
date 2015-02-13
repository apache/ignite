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

package org.apache.ignite.internal.processors.fs;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.ignitefs.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;

import java.util.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.ignitefs.IgfsMode.*;

/**
 * Test for GGFS metrics.
 */
public class IgfsMetricsSelfTest extends IgfsCommonAbstractTest {
    /** Primary GGFS name. */
    private static final String GGFS_PRIMARY = "ggfs-primary";

    /** Primary GGFS name. */
    private static final String GGFS_SECONDARY = "ggfs-secondary";

    /** Secondary file system REST endpoint configuration map. */
    private static final Map<String, String> SECONDARY_REST_CFG = new HashMap<String, String>(){{
        put("type", "tcp");
        put("port", "11500");
    }};

    /** Test nodes count. */
    private static final int NODES_CNT = 3;

    /** IP finder for the grid with the primary file system. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

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
        IgfsConfiguration ggfsCfg = new IgfsConfiguration();

        ggfsCfg.setDataCacheName("dataCache");
        ggfsCfg.setMetaCacheName("metaCache");
        ggfsCfg.setName(GGFS_PRIMARY);
        ggfsCfg.setBlockSize(PRIMARY_BLOCK_SIZE);
        ggfsCfg.setDefaultMode(PRIMARY);
        ggfsCfg.setSecondaryFileSystem(ggfsSecondary);

        Map<String, IgfsMode> pathModes = new HashMap<>();

        pathModes.put("/fileRemote", DUAL_SYNC);

        ggfsCfg.setPathModes(pathModes);

        CacheConfiguration dataCacheCfg = defaultCacheConfiguration();

        dataCacheCfg.setName("dataCache");
        dataCacheCfg.setCacheMode(PARTITIONED);
        dataCacheCfg.setDistributionMode(CacheDistributionMode.PARTITIONED_ONLY);
        dataCacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        dataCacheCfg.setAffinityMapper(new IgfsGroupDataBlocksKeyMapper(128));
        dataCacheCfg.setBackups(0);
        dataCacheCfg.setQueryIndexEnabled(false);
        dataCacheCfg.setAtomicityMode(TRANSACTIONAL);

        CacheConfiguration metaCacheCfg = defaultCacheConfiguration();

        metaCacheCfg.setName("metaCache");
        metaCacheCfg.setCacheMode(REPLICATED);
        metaCacheCfg.setDistributionMode(CacheDistributionMode.PARTITIONED_ONLY);
        metaCacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        metaCacheCfg.setQueryIndexEnabled(false);
        metaCacheCfg.setAtomicityMode(TRANSACTIONAL);

        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setGridName("grid-" + idx);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

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
        IgfsConfiguration ggfsCfg = new IgfsConfiguration();

        ggfsCfg.setDataCacheName("dataCache");
        ggfsCfg.setMetaCacheName("metaCache");
        ggfsCfg.setName(GGFS_SECONDARY);
        ggfsCfg.setBlockSize(SECONDARY_BLOCK_SIZE);
        ggfsCfg.setDefaultMode(PRIMARY);
        ggfsCfg.setIpcEndpointConfiguration(SECONDARY_REST_CFG);

        CacheConfiguration dataCacheCfg = defaultCacheConfiguration();

        dataCacheCfg.setName("dataCache");
        dataCacheCfg.setCacheMode(PARTITIONED);
        dataCacheCfg.setDistributionMode(CacheDistributionMode.PARTITIONED_ONLY);
        dataCacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        dataCacheCfg.setAffinityMapper(new IgfsGroupDataBlocksKeyMapper(128));
        dataCacheCfg.setBackups(0);
        dataCacheCfg.setQueryIndexEnabled(false);
        dataCacheCfg.setAtomicityMode(TRANSACTIONAL);

        CacheConfiguration metaCacheCfg = defaultCacheConfiguration();

        metaCacheCfg.setName("metaCache");
        metaCacheCfg.setCacheMode(REPLICATED);
        metaCacheCfg.setDistributionMode(CacheDistributionMode.PARTITIONED_ONLY);
        metaCacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        metaCacheCfg.setQueryIndexEnabled(false);
        metaCacheCfg.setAtomicityMode(TRANSACTIONAL);

        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setGridName("grid-secondary");

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(new TcpDiscoveryVmIpFinder(true));

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
        IgniteFs fs = ggfsPrimary[0];

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
        IgfsEx ggfs = (IgfsEx)ggfsPrimary[0];

        IgfsPath fileRemote = new IgfsPath("/fileRemote");
        IgfsPath file1 = new IgfsPath("/file1");
        IgfsPath file2 = new IgfsPath("/file2");

        // Create remote file and write some data to it.
        IgfsOutputStream out = ggfsSecondary.create(fileRemote, 256, true, null, 1, 256, null);

        int rmtBlockSize = ggfsSecondary.info(fileRemote).blockSize();

        out.write(new byte[rmtBlockSize]);
        out.close();

        // Start metrics measuring.
        IgfsMetrics initMetrics = ggfs.metrics();

        // Create empty file.
        ggfs.create(file1, 256, true, null, 1, 256, null).close();

        int blockSize = ggfs.info(file1).blockSize();

        checkBlockMetrics(initMetrics, ggfs.metrics(), 0, 0, 0, 0, 0, 0);

        // Write two blocks to the file.
        IgfsOutputStream os = ggfs.append(file1, false);
        os.write(new byte[blockSize * 2]);
        os.close();

        checkBlockMetrics(initMetrics, ggfs.metrics(), 0, 0, 0, 2, 0, blockSize * 2);

        // Write one more file (one block).
        os = ggfs.create(file2, 256, true, null, 1, 256, null);
        os.write(new byte[blockSize]);
        os.close();

        checkBlockMetrics(initMetrics, ggfs.metrics(), 0, 0, 0, 3, 0, blockSize * 3);

        // Read data from the first file.
        IgfsInputStreamAdapter is = ggfs.open(file1);
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

        IgfsMetrics metrics = ggfs.metrics();

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
