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

import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.Callable;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.igfs.IgfsBlockLocation;
import org.apache.ignite.igfs.IgfsException;
import org.apache.ignite.igfs.IgfsGroupDataBlocksKeyMapper;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.testframework.GridTestUtils.sleepAndIncrement;

/**
 * {@link IgfsDataManager} test case.
 */
public class IgfsDataManagerSelfTest extends IgfsCommonAbstractTest {
    /** Test IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Meta-information cache name. */
    private static final String META_CACHE_NAME = "meta";

    /** Data cache name. */
    private static final String DATA_CACHE_NAME = "partitioned";

    /** Groups count for data blocks. */
    private static final int DATA_BLOCK_GROUP_CNT = 2;

    /** IGFS block size. */
    private static final int BLOCK_SIZE = 32 * 1024;

    /** Test nodes count. */
    private static final int NODES_CNT = 4;

    /** Busy wait sleep interval in milliseconds. */
    private static final int BUSY_WAIT_SLEEP_INTERVAL = 200;

    /** IGFS block size. */
    private static final int IGFS_BLOCK_SIZE = 64 * 1024;

    /** Random numbers generator. */
    private final SecureRandom rnd = new SecureRandom();

    /** Data manager to test. */
    private IgfsDataManager mgr;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        IgfsEx igfs = (IgfsEx)grid(0).fileSystem("igfs");

        mgr = igfs.context().data();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCacheConfiguration(cacheConfiguration(META_CACHE_NAME), cacheConfiguration(DATA_CACHE_NAME));

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        FileSystemConfiguration igfsCfg = new FileSystemConfiguration();

        igfsCfg.setMetaCacheName(META_CACHE_NAME);
        igfsCfg.setDataCacheName(DATA_CACHE_NAME);
        igfsCfg.setBlockSize(IGFS_BLOCK_SIZE);
        igfsCfg.setName("igfs");
        igfsCfg.setBlockSize(BLOCK_SIZE);

        cfg.setFileSystemConfiguration(igfsCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    protected CacheConfiguration cacheConfiguration(String cacheName) {
        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setName(cacheName);

        if (META_CACHE_NAME.equals(cacheName))
            cacheCfg.setCacheMode(REPLICATED);
        else {
            cacheCfg.setCacheMode(PARTITIONED);
            cacheCfg.setNearConfiguration(null);

            cacheCfg.setBackups(0);
            cacheCfg.setAffinityMapper(new IgfsGroupDataBlocksKeyMapper(DATA_BLOCK_GROUP_CNT));
        }

        cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);

        return cacheCfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        for (int i = 0; i < NODES_CNT; i++) {
            grid(i).cachex(META_CACHE_NAME).clear();

            grid(i).cachex(DATA_CACHE_NAME).clear();
        }
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(NODES_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * Test file system structure in meta-cache.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("ConstantConditions")
    public void testDataStoring() throws Exception {
        for (int i = 0; i < 10; i++) {
            IgfsPath path = new IgfsPath();
            IgfsFileInfo info = new IgfsFileInfo(200, null, false, null);

            assertNull(mgr.dataBlock(info, path, 0, null).get());

            byte[] data = new byte[rnd.nextInt(20000) + 5];

            rnd.nextBytes(data);

            IgniteInternalFuture<Boolean> fut = mgr.writeStart(info);

            expectsStoreFail(info, data, "Not enough space reserved to store data");

            info = new IgfsFileInfo(info, info.length() + data.length - 3);

            expectsStoreFail(info, data, "Not enough space reserved to store data");

            info = new IgfsFileInfo(info, info.length() + 3);

            IgfsFileAffinityRange range = new IgfsFileAffinityRange();

            byte[] remainder = mgr.storeDataBlocks(info, info.length(), null, 0, ByteBuffer.wrap(data), true,
                range, null);

            assert remainder == null;

            mgr.writeClose(info);

            fut.get(3000);

            for (int j = 0; j < NODES_CNT; j++) {
                GridCacheContext<Object, Object> ctx = GridTestUtils.getFieldValue(grid(j).cachex(DATA_CACHE_NAME),
                    "ctx");
                Collection<IgniteInternalTx> txs = ctx.tm().txs();

                assert txs.isEmpty() : "Incomplete transactions: " + txs;
            }

            // Validate data stored in cache.
            for (int pos = 0, block = 0; pos < info.length(); block++) {
                byte[] stored = mgr.dataBlock(info, path, block, null).get();

                assertNotNull("Expects data exist [data.length=" + data.length + ", block=" + block + ']', stored);

                for (int j = 0; j < stored.length; j++)
                    assertEquals(stored[j], data[pos + j]);

                pos += stored.length;
            }

            mgr.delete(info);

            long nIters = getTestTimeout() / BUSY_WAIT_SLEEP_INTERVAL;

            assert nIters < Integer.MAX_VALUE;

            boolean rmvBlocks = false;

            // Wait for all blocks to be removed.
            for (int j = 0; j < nIters && !rmvBlocks; j = sleepAndIncrement(BUSY_WAIT_SLEEP_INTERVAL, j)) {
                boolean b = true;

                for (long block = 0; block < info.blocksCount(); block++)
                    b &= mgr.dataBlock(info, path, block, null).get() == null;

                rmvBlocks = b;
            }

            assertTrue("All blocks should be removed from cache.", rmvBlocks);
        }
    }

    /**
     * Test file system structure in meta-cache.
     *
     * @throws Exception If failed.
     */
    public void testDataStoringRemainder() throws Exception {
        final int blockSize = IGFS_BLOCK_SIZE;

        for (int i = 0; i < 10; i++) {
            IgfsPath path = new IgfsPath();
            IgfsFileInfo info = new IgfsFileInfo(blockSize, null, false, null);

            assertNull(mgr.dataBlock(info, path, 0, null).get());

            byte[] data = new byte[blockSize];

            rnd.nextBytes(data);

            byte[] remainder = new byte[blockSize / 2];

            rnd.nextBytes(remainder);

            info = new IgfsFileInfo(info, info.length() + data.length + remainder.length);

            IgniteInternalFuture<Boolean> fut = mgr.writeStart(info);

            IgfsFileAffinityRange range = new IgfsFileAffinityRange();

            byte[] left = mgr.storeDataBlocks(info, info.length(), remainder, remainder.length, ByteBuffer.wrap(data),
                false, range, null);

            assert left.length == blockSize / 2;

            byte[] remainder2 = new byte[blockSize / 2];

            info = new IgfsFileInfo(info, info.length() + remainder2.length);

            byte[] left2 = mgr.storeDataBlocks(info, info.length(), left, left.length, ByteBuffer.wrap(remainder2),
                false, range, null);

            assert left2 == null;

            mgr.writeClose(info);

            fut.get(3000);

            for (int j = 0; j < NODES_CNT; j++) {
                GridCacheContext<Object, Object> ctx = GridTestUtils.getFieldValue(grid(j).cachex(DATA_CACHE_NAME),
                    "ctx");
                Collection<IgniteInternalTx> txs = ctx.tm().txs();

                assert txs.isEmpty() : "Incomplete transactions: " + txs;
            }

            byte[] concat = U.join(remainder, data, remainder2);

            // Validate data stored in cache.
            for (int pos = 0, block = 0; pos < info.length(); block++) {
                byte[] stored = mgr.dataBlock(info, path, block, null).get();

                assertNotNull("Expects data exist [data.length=" + concat.length + ", block=" + block + ']', stored);

                for (int j = 0; j < stored.length; j++)
                    assertEquals(stored[j], concat[pos + j]);

                pos += stored.length;
            }

            mgr.delete(info);

            long nIters = getTestTimeout() / BUSY_WAIT_SLEEP_INTERVAL;

            assert nIters < Integer.MAX_VALUE;

            boolean rmvBlocks = false;

            // Wait for all blocks to be removed.
            for (int j = 0; j < nIters && !rmvBlocks; j = sleepAndIncrement(BUSY_WAIT_SLEEP_INTERVAL, j)) {
                boolean b = true;

                for (long block = 0; block < info.blocksCount(); block++)
                    b &= mgr.dataBlock(info, path, block, null).get() == null;

                rmvBlocks = b;
            }

            assertTrue("All blocks should be removed from cache.", rmvBlocks);
        }
    }

    /** @throws Exception If failed. */
    public void testDataStoringFlush() throws Exception {
        final int blockSize = IGFS_BLOCK_SIZE;
        final int writesCnt = 64;

        for (int i = 0; i < 10; i++) {
            IgfsPath path = new IgfsPath();
            IgfsFileInfo info = new IgfsFileInfo(blockSize, null, false, null);

            IgfsFileAffinityRange range = new IgfsFileAffinityRange();

            assertNull(mgr.dataBlock(info, path, 0, null).get());

            int chunkSize = blockSize / 4;

            byte[] data = new byte[chunkSize];

            info = new IgfsFileInfo(info, info.length() + data.length * writesCnt);

            IgniteInternalFuture<Boolean> fut = mgr.writeStart(info);

            for (int j = 0; j < 64; j++) {
                Arrays.fill(data, (byte)(j / 4));

                byte[] left = mgr.storeDataBlocks(info, (j + 1) * chunkSize, null, 0, ByteBuffer.wrap(data),
                    true, range, null);

                assert left == null : "No remainder should be returned if flush is true: " + Arrays.toString(left);
            }

            mgr.writeClose(info);

            assertTrue(range.regionEqual(new IgfsFileAffinityRange(0, writesCnt * chunkSize - 1, null)));

            fut.get(3000);

            for (int j = 0; j < NODES_CNT; j++) {
                GridCacheContext<Object, Object> ctx = GridTestUtils.getFieldValue(grid(j).cachex(DATA_CACHE_NAME),
                    "ctx");
                Collection<IgniteInternalTx> txs = ctx.tm().txs();

                assert txs.isEmpty() : "Incomplete transactions: " + txs;
            }

            // Validate data stored in cache.
            for (int pos = 0, block = 0; pos < info.length(); block++) {
                byte[] stored = mgr.dataBlock(info, path, block, null).get();

                assertNotNull("Expects data exist [block=" + block + ']', stored);

                for (byte b : stored)
                    assertEquals(b, (byte)block);

                pos += stored.length;
            }

            IgniteInternalFuture<Object> delFut = mgr.delete(info);

            delFut.get();

            for (long block = 0; block < info.blocksCount(); block++)
                assertNull(mgr.dataBlock(info, path, block, null).get());
        }
    }

    /**
     * Test affinity.
     *
     * @throws Exception If failed.
     */
    public void testAffinity() throws Exception {
        final int blockSize = 10;
        final int grpSize = blockSize * DATA_BLOCK_GROUP_CNT;

        //IgfsFileInfo info = new IgfsFileInfo(blockSize, 0);
        IgfsFileInfo info = new IgfsFileInfo(blockSize, 1024 * 1024, null, null, false, null);

        for (int pos = 0; pos < 5 * grpSize; pos++) {
            assertEquals("Expects no affinity for zero length.", Collections.<IgfsBlockLocation>emptyList(),
                mgr.affinity(info, pos, 0));

            // Expects grouped data blocks are interpreted as a single block location.
            // And no guaranties for blocks out of the group.
            for (int len = 1, maxLen = grpSize - pos % grpSize; len < maxLen; len++) {
                Collection<IgfsBlockLocation> aff = mgr.affinity(info, pos, len);

                assertEquals("Unexpected affinity: " + aff, 1, aff.size());

                IgfsBlockLocation loc = F.first(aff);

                assertEquals("Unexpected block location: " + loc, pos, loc.start());
                assertEquals("Unexpected block location: " + loc, len, loc.length());
            }

            // Validate ranges.
            for (int len = grpSize * 4 + 1, maxLen = 5 * grpSize - pos % grpSize; len < maxLen; len++) {
                Collection<IgfsBlockLocation> aff = mgr.affinity(info, pos, len);

                assertTrue("Unexpected affinity [aff=" + aff + ", pos=" + pos + ", len=" + len + ']', aff.size() <= 5);

                IgfsBlockLocation first = F.first(aff);

                assertEquals("Unexpected the first block location [aff=" + aff + ", pos=" + pos + ", len=" + len + ']',
                    pos, first.start());

                assertTrue("Unexpected the first block location [aff=" + aff + ", pos=" + pos + ", len=" + len + ']',
                    first.length() >= grpSize - pos % grpSize);

                IgfsBlockLocation last = F.last(aff);

                assertTrue("Unexpected the last block location [aff=" + aff + ", pos=" + pos + ", len=" + len + ']',
                    last.start() <= (pos / grpSize + 4) * grpSize);

                assertTrue("Unexpected the last block location [aff=" + aff + ", pos=" + pos + ", len=" + len + ']',
                    last.length() >= (pos + len - 1) % grpSize + 1);
            }
        }
    }

    /** @throws Exception If failed. */
    public void testAffinity2() throws Exception {
        int blockSize = BLOCK_SIZE;

        IgfsFileInfo info = new IgfsFileInfo(blockSize, 1024 * 1024, null, null, false, null);

        Collection<IgfsBlockLocation> affinity = mgr.affinity(info, 0, info.length());

        for (IgfsBlockLocation loc : affinity) {
            info("Going to check IGFS block location: " + loc);

            int block = (int)(loc.start() / blockSize);

            int endPos;

            do {
                IgfsBlockKey key = new IgfsBlockKey(info.id(), null, false, block);

                ClusterNode affNode = grid(0).affinity(DATA_CACHE_NAME).mapKeyToNode(key);

                assertTrue("Failed to find node in affinity [dataMgr=" + loc.nodeIds() +
                    ", nodeId=" + affNode.id() + ", block=" + block + ']', loc.nodeIds().contains(affNode.id()));

                endPos = (block + 1) * blockSize;

                block++;
            }
            while (endPos < loc.start() + loc.length());
        }
    }

    /** @throws Exception If failed. */
    public void testAffinityFileMap() throws Exception {
        int blockSize = BLOCK_SIZE;

        IgfsFileInfo info = new IgfsFileInfo(blockSize, 1024 * 1024, null, null, false, null);

        IgniteUuid affKey = IgniteUuid.randomUuid();

        IgfsFileMap map = new IgfsFileMap();

        map.addRange(new IgfsFileAffinityRange(3 * BLOCK_SIZE, 5 * BLOCK_SIZE - 1, affKey));
        map.addRange(new IgfsFileAffinityRange(13 * BLOCK_SIZE, 17 * BLOCK_SIZE - 1, affKey));

        info.fileMap(map);

        Collection<IgfsBlockLocation> affinity = mgr.affinity(info, 0, info.length());

        checkAffinity(blockSize, info, affinity);

        // Check from middle of range.
        affinity = mgr.affinity(info, 3 * BLOCK_SIZE + BLOCK_SIZE / 2, info.length());

        checkAffinity(blockSize, info, affinity);

        // Check from middle of last range.
        affinity = mgr.affinity(info, 14 * BLOCK_SIZE, info.length());

        checkAffinity(blockSize, info, affinity);

        // Check inside one range.
        affinity = mgr.affinity(info, 14 * BLOCK_SIZE, 2 * BLOCK_SIZE);

        checkAffinity(blockSize, info, affinity);

        // Check outside last range.
        affinity = mgr.affinity(info, 18 * BLOCK_SIZE, info.length());

        checkAffinity(blockSize, info, affinity);
    }

    /**
     * Checks affinity validity.
     *
     * @param blockSize Block size.
     * @param info File info.
     * @param affinity Affinity block locations to check.
     */
    private void checkAffinity(int blockSize, IgfsFileInfo info, Iterable<IgfsBlockLocation> affinity) {
        for (IgfsBlockLocation loc : affinity) {
            info("Going to check IGFS block location: " + loc);

            int block = (int)(loc.start() / blockSize);

            int endPos;

            do {
                IgfsBlockKey key = new IgfsBlockKey(info.id(),
                    info.fileMap().affinityKey(block * blockSize, false), false, block);

                ClusterNode affNode = grid(0).affinity(DATA_CACHE_NAME).mapKeyToNode(key);

                assertTrue("Failed to find node in affinity [dataMgr=" + loc.nodeIds() +
                    ", nodeId=" + affNode.id() + ", block=" + block + ']', loc.nodeIds().contains(affNode.id()));

                endPos = (block + 1) * blockSize;

                block++;
            }
            while (endPos < loc.start() + loc.length());
        }
    }

    /**
     * Test expected failures for 'store' operation.
     *
     * @param reserved Reserved file info.
     * @param data Data to store.
     * @param msg Expected failure message.
     */
    private void expectsStoreFail(final IgfsFileInfo reserved, final byte[] data, @Nullable String msg) {
        GridTestUtils.assertThrows(log, new Callable() {
            @Override public Object call() throws Exception {
                IgfsFileAffinityRange range = new IgfsFileAffinityRange();

                mgr.storeDataBlocks(reserved, reserved.length(), null, 0, ByteBuffer.wrap(data), false, range, null);

                return null;
            }
        }, IgfsException.class, msg);
    }

    /**
     * Test expected failures for 'delete' operation.
     *
     * @param fileInfo File to delete data for.
     * @param msg Expected failure message.
     */
    private void expectsDeleteFail(final IgfsFileInfo fileInfo, @Nullable String msg) {
        GridTestUtils.assertThrows(log, new Callable() {
            @Override public Object call() throws Exception {
                mgr.delete(fileInfo);

                return null;
            }
        }, IgfsException.class, msg);
    }

    /**
     * Test expected failures for 'affinity' operation.
     *
     * @param info File info to resolve affinity nodes for.
     * @param start Start position in the file.
     * @param len File part length to get affinity for.
     * @param msg Expected failure message.
     */
    private void expectsAffinityFail(final IgfsFileInfo info, final long start, final long len,
        @Nullable String msg) {
        GridTestUtils.assertThrows(log, new Callable() {
            @Override public Object call() throws Exception {
                mgr.affinity(info, start, len);

                return null;
            }
        }, IgfsException.class, msg);
    }
}