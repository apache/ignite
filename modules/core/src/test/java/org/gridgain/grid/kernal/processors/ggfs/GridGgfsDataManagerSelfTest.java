/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.ggfs;

import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;
import org.jetbrains.annotations.*;

import java.nio.*;
import java.security.*;
import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.testframework.GridTestUtils.*;

/**
 * {@link GridGgfsDataManager} test case.
 */
public class GridGgfsDataManagerSelfTest extends GridGgfsCommonAbstractTest {
    /** Test IP finder. */
    private static final GridTcpDiscoveryIpFinder IP_FINDER = new GridTcpDiscoveryVmIpFinder(true);

    /** Meta-information cache name. */
    private static final String META_CACHE_NAME = "meta";

    /** Data cache name. */
    private static final String DATA_CACHE_NAME = "partitioned";

    /** Groups count for data blocks. */
    private static final int DATA_BLOCK_GROUP_CNT = 2;

    /** GGFS block size. */
    private static final int BLOCK_SIZE = 32 * 1024;

    /** Test nodes count. */
    private static final int NODES_CNT = 4;

    /** Busy wait sleep interval in milliseconds. */
    private static final int BUSY_WAIT_SLEEP_INTERVAL = 200;

    /** GGFS block size. */
    private static final int GGFS_BLOCK_SIZE = 64 * 1024;

    /** Random numbers generator. */
    private final SecureRandom rnd = new SecureRandom();

    /** Data manager to test. */
    private GridGgfsDataManager mgr;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        GridGgfsEx ggfs = (GridGgfsEx)grid(0).ggfs("ggfs");

        mgr = ggfs.context().data();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCacheConfiguration(cacheConfiguration(META_CACHE_NAME), cacheConfiguration(DATA_CACHE_NAME));

        GridTcpDiscoverySpi discoSpi = new GridTcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        GridGgfsConfiguration ggfsCfg = new GridGgfsConfiguration();

        ggfsCfg.setMetaCacheName(META_CACHE_NAME);
        ggfsCfg.setDataCacheName(DATA_CACHE_NAME);
        ggfsCfg.setBlockSize(GGFS_BLOCK_SIZE);
        ggfsCfg.setName("ggfs");
        ggfsCfg.setBlockSize(BLOCK_SIZE);

        cfg.setGgfsConfiguration(ggfsCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    protected GridCacheConfiguration cacheConfiguration(String cacheName) {
        GridCacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setName(cacheName);

        if (META_CACHE_NAME.equals(cacheName))
            cacheCfg.setCacheMode(REPLICATED);
        else {
            cacheCfg.setCacheMode(PARTITIONED);
            cacheCfg.setDistributionMode(GridCacheDistributionMode.PARTITIONED_ONLY);

            cacheCfg.setBackups(0);
            cacheCfg.setAffinityMapper(new GridGgfsGroupDataBlocksKeyMapper(DATA_BLOCK_GROUP_CNT));
        }

        cacheCfg.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);
        cacheCfg.setQueryIndexEnabled(false);

        return cacheCfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        for (int i = 0; i < NODES_CNT; i++) {
            grid(i).cachex(META_CACHE_NAME).clearAll();

            grid(i).cachex(DATA_CACHE_NAME).clearAll();
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
            GridGgfsPath path = new GridGgfsPath();
            GridGgfsFileInfo info = new GridGgfsFileInfo(200, null, false, null);

            assertNull(mgr.dataBlock(info, path, 0, null).get());

            byte[] data = new byte[rnd.nextInt(20000) + 5];

            rnd.nextBytes(data);

            GridFuture<Boolean> fut = mgr.writeStart(info);

            expectsStoreFail(info, data, "Not enough space reserved to store data");

            info = new GridGgfsFileInfo(info, info.length() + data.length - 3);

            expectsStoreFail(info, data, "Not enough space reserved to store data");

            info = new GridGgfsFileInfo(info, info.length() + 3);

            GridGgfsFileAffinityRange range = new GridGgfsFileAffinityRange();

            byte[] remainder = mgr.storeDataBlocks(info, info.length(), null, 0, ByteBuffer.wrap(data), true,
                range, null);

            assert remainder == null;

            mgr.writeClose(info);

            fut.get(3000);

            for (int j = 0; j < NODES_CNT; j++) {
                GridCacheContext<Object, Object> ctx = GridTestUtils.getFieldValue(grid(j).cachex(DATA_CACHE_NAME),
                    "ctx");
                Collection<GridCacheTxEx<Object, Object>> txs = ctx.tm().txs();

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
        final int blockSize = GGFS_BLOCK_SIZE;

        for (int i = 0; i < 10; i++) {
            GridGgfsPath path = new GridGgfsPath();
            GridGgfsFileInfo info = new GridGgfsFileInfo(blockSize, null, false, null);

            assertNull(mgr.dataBlock(info, path, 0, null).get());

            byte[] data = new byte[blockSize];

            rnd.nextBytes(data);

            byte[] remainder = new byte[blockSize / 2];

            rnd.nextBytes(remainder);

            info = new GridGgfsFileInfo(info, info.length() + data.length + remainder.length);

            GridFuture<Boolean> fut = mgr.writeStart(info);

            GridGgfsFileAffinityRange range = new GridGgfsFileAffinityRange();

            byte[] left = mgr.storeDataBlocks(info, info.length(), remainder, remainder.length, ByteBuffer.wrap(data),
                false, range, null);

            assert left.length == blockSize / 2;

            byte[] remainder2 = new byte[blockSize / 2];

            info = new GridGgfsFileInfo(info, info.length() + remainder2.length);

            byte[] left2 = mgr.storeDataBlocks(info, info.length(), left, left.length, ByteBuffer.wrap(remainder2),
                false, range, null);

            assert left2 == null;

            mgr.writeClose(info);

            fut.get(3000);

            for (int j = 0; j < NODES_CNT; j++) {
                GridCacheContext<Object, Object> ctx = GridTestUtils.getFieldValue(grid(j).cachex(DATA_CACHE_NAME),
                    "ctx");
                Collection<GridCacheTxEx<Object, Object>> txs = ctx.tm().txs();

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
        final int blockSize = GGFS_BLOCK_SIZE;
        final int writesCnt = 64;

        for (int i = 0; i < 10; i++) {
            GridGgfsPath path = new GridGgfsPath();
            GridGgfsFileInfo info = new GridGgfsFileInfo(blockSize, null, false, null);

            GridGgfsFileAffinityRange range = new GridGgfsFileAffinityRange();

            assertNull(mgr.dataBlock(info, path, 0, null).get());

            int chunkSize = blockSize / 4;

            byte[] data = new byte[chunkSize];

            info = new GridGgfsFileInfo(info, info.length() + data.length * writesCnt);

            GridFuture<Boolean> fut = mgr.writeStart(info);

            for (int j = 0; j < 64; j++) {
                Arrays.fill(data, (byte)(j / 4));

                byte[] left = mgr.storeDataBlocks(info, (j + 1) * chunkSize, null, 0, ByteBuffer.wrap(data),
                    true, range, null);

                assert left == null : "No remainder should be returned if flush is true: " + Arrays.toString(left);
            }

            mgr.writeClose(info);

            assertTrue(range.regionEqual(new GridGgfsFileAffinityRange(0, writesCnt * chunkSize - 1, null)));

            fut.get(3000);

            for (int j = 0; j < NODES_CNT; j++) {
                GridCacheContext<Object, Object> ctx = GridTestUtils.getFieldValue(grid(j).cachex(DATA_CACHE_NAME),
                    "ctx");
                Collection<GridCacheTxEx<Object, Object>> txs = ctx.tm().txs();

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

            GridFuture<Object> delFut = mgr.delete(info);

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

        //GridGgfsFileInfo info = new GridGgfsFileInfo(blockSize, 0);
        GridGgfsFileInfo info = new GridGgfsFileInfo(blockSize, 1024 * 1024, null, null, false, null);

        for (int pos = 0; pos < 5 * grpSize; pos++) {
            assertEquals("Expects no affinity for zero length.", Collections.<GridGgfsBlockLocation>emptyList(),
                mgr.affinity(info, pos, 0));

            // Expects grouped data blocks are interpreted as a single block location.
            // And no guaranties for blocks out of the group.
            for (int len = 1, maxLen = grpSize - pos % grpSize; len < maxLen; len++) {
                Collection<GridGgfsBlockLocation> aff = mgr.affinity(info, pos, len);

                assertEquals("Unexpected affinity: " + aff, 1, aff.size());

                GridGgfsBlockLocation loc = F.first(aff);

                assertEquals("Unexpected block location: " + loc, pos, loc.start());
                assertEquals("Unexpected block location: " + loc, len, loc.length());
            }

            // Validate ranges.
            for (int len = grpSize * 4 + 1, maxLen = 5 * grpSize - pos % grpSize; len < maxLen; len++) {
                Collection<GridGgfsBlockLocation> aff = mgr.affinity(info, pos, len);

                assertTrue("Unexpected affinity [aff=" + aff + ", pos=" + pos + ", len=" + len + ']', aff.size() <= 5);

                GridGgfsBlockLocation first = F.first(aff);

                assertEquals("Unexpected the first block location [aff=" + aff + ", pos=" + pos + ", len=" + len + ']',
                    pos, first.start());

                assertTrue("Unexpected the first block location [aff=" + aff + ", pos=" + pos + ", len=" + len + ']',
                    first.length() >= grpSize - pos % grpSize);

                GridGgfsBlockLocation last = F.last(aff);

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

        GridGgfsFileInfo info = new GridGgfsFileInfo(blockSize, 1024 * 1024, null, null, false, null);

        Collection<GridGgfsBlockLocation> affinity = mgr.affinity(info, 0, info.length());

        for (GridGgfsBlockLocation loc : affinity) {
            info("Going to check GGFS block location: " + loc);

            int block = (int)(loc.start() / blockSize);

            int endPos;

            do {
                GridGgfsBlockKey key = new GridGgfsBlockKey(info.id(), null, false, block);

                ClusterNode affNode = grid(0).cachex(DATA_CACHE_NAME).affinity().mapKeyToNode(key);

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

        GridGgfsFileInfo info = new GridGgfsFileInfo(blockSize, 1024 * 1024, null, null, false, null);

        IgniteUuid affKey = IgniteUuid.randomUuid();

        GridGgfsFileMap map = new GridGgfsFileMap();

        map.addRange(new GridGgfsFileAffinityRange(3 * BLOCK_SIZE, 5 * BLOCK_SIZE - 1, affKey));
        map.addRange(new GridGgfsFileAffinityRange(13 * BLOCK_SIZE, 17 * BLOCK_SIZE - 1, affKey));

        info.fileMap(map);

        Collection<GridGgfsBlockLocation> affinity = mgr.affinity(info, 0, info.length());

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
    private void checkAffinity(int blockSize, GridGgfsFileInfo info, Iterable<GridGgfsBlockLocation> affinity) {
        GridCache<Object, Object> dataCache = grid(0).cachex(DATA_CACHE_NAME);

        for (GridGgfsBlockLocation loc : affinity) {
            info("Going to check GGFS block location: " + loc);

            int block = (int)(loc.start() / blockSize);

            int endPos;

            do {
                GridGgfsBlockKey key = new GridGgfsBlockKey(info.id(),
                    info.fileMap().affinityKey(block * blockSize, false), false, block);

                ClusterNode affNode = dataCache.affinity().mapKeyToNode(key);

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
    private void expectsStoreFail(final GridGgfsFileInfo reserved, final byte[] data, @Nullable String msg) {
        GridTestUtils.assertThrows(log, new Callable() {
            @Override public Object call() throws Exception {
                GridGgfsFileAffinityRange range = new GridGgfsFileAffinityRange();

                mgr.storeDataBlocks(reserved, reserved.length(), null, 0, ByteBuffer.wrap(data), false, range, null);

                return null;
            }
        }, GridGgfsException.class, msg);
    }

    /**
     * Test expected failures for 'delete' operation.
     *
     * @param fileInfo File to delete data for.
     * @param msg Expected failure message.
     */
    private void expectsDeleteFail(final GridGgfsFileInfo fileInfo, @Nullable String msg) {
        GridTestUtils.assertThrows(log, new Callable() {
            @Override public Object call() throws Exception {
                mgr.delete(fileInfo);

                return null;
            }
        }, GridGgfsException.class, msg);
    }

    /**
     * Test expected failures for 'affinity' operation.
     *
     * @param info File info to resolve affinity nodes for.
     * @param start Start position in the file.
     * @param len File part length to get affinity for.
     * @param msg Expected failure message.
     */
    private void expectsAffinityFail(final GridGgfsFileInfo info, final long start, final long len,
        @Nullable String msg) {
        GridTestUtils.assertThrows(log, new Callable() {
            @Override public Object call() throws Exception {
                mgr.affinity(info, start, len);

                return null;
            }
        }, GridGgfsException.class, msg);
    }
}
