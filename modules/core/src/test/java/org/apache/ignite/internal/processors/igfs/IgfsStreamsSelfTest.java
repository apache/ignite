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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.igfs.IgfsBlockLocation;
import org.apache.ignite.igfs.IgfsFile;
import org.apache.ignite.igfs.IgfsGroupDataBlocksKeyMapper;
import org.apache.ignite.igfs.IgfsInputStream;
import org.apache.ignite.igfs.IgfsOutputStream;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.util.typedef.CAX;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;
import static org.apache.ignite.testframework.GridTestUtils.runMultiThreaded;

/**
 * Tests for IGFS streams content.
 */
public class IgfsStreamsSelfTest extends IgfsCommonAbstractTest {
    /** Test IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Meta-information cache name. */
    private static final String META_CACHE_NAME = "replicated";

    /** Data cache name. */
    public static final String DATA_CACHE_NAME = "data";

    /** Group size. */
    public static final int CFG_GRP_SIZE = 128;

    /** Pre-configured block size. */
    private static final int CFG_BLOCK_SIZE = 64000;

    /** Number of threads to test parallel readings. */
    private static final int WRITING_THREADS_CNT = 5;

    /** Number of threads to test parallel readings. */
    private static final int READING_THREADS_CNT = 5;

    /** Test nodes count. */
    private static final int NODES_CNT = 4;

    /** Number of retries for async ops. */
    public static final int ASSERT_RETRIES = 100;

    /** Delay between checks for async ops. */
    public static final int ASSERT_RETRY_INTERVAL = 100;

    /** File system to test. */
    private IgniteFileSystem fs;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(NODES_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        if (NODES_CNT <= 0)
            return;

        // Initialize FS.
        fs = grid(0).fileSystem("igfs");

        // Cleanup FS.
        fs.format();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setLateAffinityAssignment(false);
        cfg.setCacheConfiguration(cacheConfiguration(META_CACHE_NAME), cacheConfiguration(DATA_CACHE_NAME));

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        FileSystemConfiguration igfsCfg = new FileSystemConfiguration();

        igfsCfg.setMetaCacheName(META_CACHE_NAME);
        igfsCfg.setDataCacheName(DATA_CACHE_NAME);
        igfsCfg.setName("igfs");
        igfsCfg.setBlockSize(CFG_BLOCK_SIZE);
        igfsCfg.setFragmentizerEnabled(true);

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
            cacheCfg.setAffinityMapper(new IgfsGroupDataBlocksKeyMapper(CFG_GRP_SIZE));
        }

        cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);
        cacheCfg.setRebalanceMode(CacheRebalanceMode.SYNC);

        return cacheCfg;
    }

    /**
     * Test IGFS construction.
     *
     * @throws IgniteCheckedException In case of exception.
     */
    public void testConfiguration() throws IgniteCheckedException {
        IgniteInternalCache metaCache = getFieldValue(fs, "meta", "metaCache");
        IgniteInternalCache dataCache = getFieldValue(fs, "data", "dataCache");

        assertNotNull(metaCache);
        assertEquals(META_CACHE_NAME, metaCache.name());
        assertEquals(REPLICATED, metaCache.configuration().getCacheMode());

        assertNotNull(dataCache);
        assertEquals(DATA_CACHE_NAME, dataCache.name());
        assertEquals(PARTITIONED, dataCache.configuration().getCacheMode());
    }

    /**
     * Test file creation.
     *
     * @throws Exception In case of exception.
     */
    public void testCreateFile() throws Exception {
        IgfsPath root = new IgfsPath("/");
        IgfsPath path = new IgfsPath("/asdf");

        long max = 100L * CFG_BLOCK_SIZE / WRITING_THREADS_CNT;

        for (long size = 0; size <= max; size = size * 15 / 10 + 1) {
            assertEquals(Collections.<IgfsPath>emptyList(), fs.listPaths(root));

            testCreateFile(path, size, new Random().nextInt());
        }
    }

    /** @throws Exception If failed. */
    public void testCreateFileColocated() throws Exception {
        IgfsPath path = new IgfsPath("/colocated");

        UUID uuid = UUID.randomUUID();

        IgniteUuid affKey;

        long idx = 0;

        while (true) {
            affKey = new IgniteUuid(uuid, idx);

            if (grid(0).affinity(DATA_CACHE_NAME).mapKeyToNode(affKey).id().equals(grid(0).localNode().id()))
                break;

            idx++;
        }

        try (IgfsOutputStream out = fs.create(path, 1024, true, affKey, 0, 1024, null)) {
            // Write 5M, should be enough to test distribution.
            for (int i = 0; i < 15; i++)
                out.write(new byte[1024 * 1024]);
        }

        IgfsFile info = fs.info(path);

        Collection<IgfsBlockLocation> affNodes = fs.affinity(path, 0, info.length());

        assertEquals(1, affNodes.size());

        Collection<UUID> nodeIds = F.first(affNodes).nodeIds();

        assertEquals(1, nodeIds.size());
        assertEquals(grid(0).localNode().id(), F.first(nodeIds));
    }

    /** @throws Exception If failed. */
    public void testCreateFileFragmented() throws Exception {
        IgfsEx impl = (IgfsEx)grid(0).fileSystem("igfs");

        IgfsFragmentizerManager fragmentizer = impl.context().fragmentizer();

        GridTestUtils.setFieldValue(fragmentizer, "fragmentizerEnabled", false);

        IgfsPath path = new IgfsPath("/file");

        try {
            IgniteFileSystem fs0 = grid(0).fileSystem("igfs");
            IgniteFileSystem fs1 = grid(1).fileSystem("igfs");
            IgniteFileSystem fs2 = grid(2).fileSystem("igfs");

            try (IgfsOutputStream out = fs0.create(path, 128, false, 1, CFG_GRP_SIZE,
                F.asMap(IgfsUtils.PROP_PREFER_LOCAL_WRITES, "true"))) {
                // 1.5 blocks
                byte[] data = new byte[CFG_BLOCK_SIZE * 3 / 2];

                Arrays.fill(data, (byte)1);

                out.write(data);
            }

            try (IgfsOutputStream out = fs1.append(path, false)) {
                // 1.5 blocks.
                byte[] data = new byte[CFG_BLOCK_SIZE * 3 / 2];

                Arrays.fill(data, (byte)2);

                out.write(data);
            }

            // After this we should have first two block colocated with grid 0 and last block colocated with grid 1.
            IgfsFileImpl fileImpl = (IgfsFileImpl)fs.info(path);

            GridCacheAdapter<Object, Object> metaCache = ((IgniteKernal)grid(0)).internalCache(META_CACHE_NAME);

            IgfsEntryInfo fileInfo = (IgfsEntryInfo)metaCache.get(fileImpl.fileId());

            IgfsFileMap map = fileInfo.fileMap();

            List<IgfsFileAffinityRange> ranges = map.ranges();

            assertEquals(2, ranges.size());

            assertTrue(ranges.get(0).startOffset() == 0);
            assertTrue(ranges.get(0).endOffset() == 2 * CFG_BLOCK_SIZE - 1);

            assertTrue(ranges.get(1).startOffset() == 2 * CFG_BLOCK_SIZE);
            assertTrue(ranges.get(1).endOffset() == 3 * CFG_BLOCK_SIZE - 1);

            // Validate data read after colocated writes.
            try (IgfsInputStream in = fs2.open(path)) {
                // Validate first part of file.
                for (int i = 0; i < CFG_BLOCK_SIZE * 3 / 2; i++)
                    assertEquals((byte)1, in.read());

                // Validate second part of file.
                for (int i = 0; i < CFG_BLOCK_SIZE * 3 / 2; i++)
                    assertEquals((byte)2, in.read());

                assertEquals(-1, in.read());
            }
        }
        finally {
            GridTestUtils.setFieldValue(fragmentizer, "fragmentizerEnabled", true);

            boolean hasData = false;

            for (int i = 0; i < NODES_CNT; i++)
                hasData |= !grid(i).cachex(DATA_CACHE_NAME).isEmpty();

            assertTrue(hasData);

            fs.delete(path, true);
        }

        GridTestUtils.retryAssert(log, ASSERT_RETRIES, ASSERT_RETRY_INTERVAL, new CAX() {
            @Override public void applyx() {
                for (int i = 0; i < NODES_CNT; i++)
                    assertTrue(grid(i).cachex(DATA_CACHE_NAME).isEmpty());
            }
        });
    }

    /**
     * Test file creation.
     *
     * @param path Path to file to store.
     * @param size Size of file to store.
     * @param salt Salt for file content generation.
     * @throws Exception In case of any exception.
     */
    private void testCreateFile(final IgfsPath path, final long size, final int salt) throws Exception {
        info("Create file [path=" + path + ", size=" + size + ", salt=" + salt + ']');

        final AtomicInteger cnt = new AtomicInteger(0);
        final Collection<IgfsPath> cleanUp = new ConcurrentLinkedQueue<>();

        long time = runMultiThreaded(new Callable<Object>() {
            @Override public Object call() throws Exception {
                int id = cnt.incrementAndGet();

                IgfsPath f = new IgfsPath(path.parent(), "asdf" + (id > 1 ? "-" + id : ""));

                try (IgfsOutputStream out = fs.create(f, 0, true, null, 0, 1024, null)) {
                    assertNotNull(out);

                    cleanUp.add(f); // Add all created into cleanup list.

                    U.copy(new IgfsTestInputStream(size, salt), out);
                }

                return null;
            }
        }, WRITING_THREADS_CNT, "perform-multi-thread-writing");

        if (time > 0) {
            double rate = size * 1000. / time / 1024 / 1024;

            info(String.format("Write file [path=%s, size=%d kB, rate=%2.1f MB/s]", path,
                WRITING_THREADS_CNT * size / 1024, WRITING_THREADS_CNT * rate));
        }

        info("Read and validate saved file: " + path);

        final InputStream expIn = new IgfsTestInputStream(size, salt);
        final IgfsInputStream actIn = fs.open(path, CFG_BLOCK_SIZE * READING_THREADS_CNT * 11 / 10);

        // Validate continuous reading of whole file.
        assertEqualStreams(expIn, actIn, size, null);

        // Validate random seek and reading.
        final Random rnd = new Random();

        runMultiThreaded(new Callable<Object>() {
            @Override public Object call() throws Exception {
                long skip = Math.abs(rnd.nextLong() % (size + 1));
                long range = Math.min(size - skip, rnd.nextInt(CFG_BLOCK_SIZE * 400));

                assertEqualStreams(new IgfsTestInputStream(size, salt), actIn, range, skip);

                return null;
            }
        }, READING_THREADS_CNT, "validate-multi-thread-reading");

        expIn.close();
        actIn.close();

        info("Get stored file info: " + path);

        IgfsFile desc = fs.info(path);

        info("Validate stored file info: " + desc);

        assertNotNull(desc);

        if (log.isDebugEnabled())
            log.debug("File descriptor: " + desc);

        Collection<IgfsBlockLocation> aff = fs.affinity(path, 0, desc.length());

        assertFalse("Affinity: " + aff, desc.length() != 0 && aff.isEmpty());

        int blockSize = desc.blockSize();

        assertEquals("File size", size, desc.length());
        assertEquals("Binary block size", CFG_BLOCK_SIZE, blockSize);
        //assertEquals("Permission", "rwxr-xr-x", desc.getPermission().toString());
        //assertEquals("Permission sticky bit marks this is file", false, desc.getPermission().getStickyBit());
        assertEquals("Type", true, desc.isFile());
        assertEquals("Type", false, desc.isDirectory());

        info("Cleanup files: " + cleanUp);

        for (IgfsPath f : cleanUp) {
            fs.delete(f, true);
            assertNull(fs.info(f));
        }
    }

    /**
     * Validate streams generate the same output.
     *
     * @param expIn Expected input stream.
     * @param actIn Actual input stream.
     * @param expSize Expected size of the streams.
     * @param seek Seek to use async position-based reading or {@code null} to use simple continuous reading.
     * @throws IOException In case of any IO exception.
     */
    private void assertEqualStreams(InputStream expIn, IgfsInputStream actIn,
        @Nullable Long expSize, @Nullable Long seek) throws IOException {
        if (seek != null)
            expIn.skip(seek);

        int bufSize = 2345;
        byte buf1[] = new byte[bufSize];
        byte buf2[] = new byte[bufSize];
        long pos = 0;

        long start = System.currentTimeMillis();

        while (true) {
            int read = (int)Math.min(bufSize, expSize - pos);

            int i1;

            if (seek == null)
                i1 = actIn.read(buf1, 0, read);
            else if (seek % 2 == 0)
                i1 = actIn.read(pos + seek, buf1, 0, read);
            else {
                i1 = read;

                actIn.readFully(pos + seek, buf1, 0, read);
            }

            // Read at least 0 byte, but don't read more then 'i1' or 'read'.
            int i2 = expIn.read(buf2, 0, Math.max(0, Math.min(i1, read)));

            if (i1 != i2) {
                fail("Expects the same data [read=" + read + ", pos=" + pos + ", seek=" + seek +
                    ", i1=" + i1 + ", i2=" + i2 + ']');
            }

            if (i1 == -1)
                break; // EOF

            // i1 == bufSize => compare buffers.
            // i1 <  bufSize => Compare part of buffers, rest of buffers are equal from previous iteration.
            assertTrue("Expects the same data [read=" + read + ", pos=" + pos + ", seek=" + seek +
                ", i1=" + i1 + ", i2=" + i2 + ']', Arrays.equals(buf1, buf2));

            if (read == 0)
                break; // Nothing more to read.

            pos += i1;
        }

        if (expSize != null)
            assertEquals(expSize.longValue(), pos);

        long time = System.currentTimeMillis() - start;

        if (time != 0 && log.isInfoEnabled()) {
            log.info(String.format("Streams were compared in continuous reading " +
                "[size=%7d, rate=%3.1f MB/sec]", expSize, expSize * 1000. / time / 1024 / 1024));
        }
    }
}
