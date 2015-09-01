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

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import org.apache.commons.io.IOUtils;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.igfs.IgfsDirectoryNotEmptyException;
import org.apache.ignite.igfs.IgfsException;
import org.apache.ignite.igfs.IgfsFile;
import org.apache.ignite.igfs.IgfsGroupDataBlocksKeyMapper;
import org.apache.ignite.igfs.IgfsInputStream;
import org.apache.ignite.igfs.IgfsOutputStream;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 * Tests for {@link IgfsProcessor}.
 */
public class IgfsProcessorSelfTest extends IgfsCommonAbstractTest {
    /** Test IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Meta-information cache name. */
    private static final String META_CACHE_NAME = "replicated";

    /** Data cache name. */
    public static final String DATA_CACHE_NAME = "data";

    /** Random numbers generator. */
    protected final SecureRandom rnd = new SecureRandom();

    /** File system. */
    protected IgniteFileSystem igfs;

    /** Meta cache. */
    private GridCacheAdapter<Object, Object> metaCache;

    /** Meta cache name. */
    private String metaCacheName;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        IgniteEx grid = grid(0);

        igfs = grid.fileSystem(igfsName());

        FileSystemConfiguration[] cfgs = grid.configuration().getFileSystemConfiguration();

        assert cfgs.length == 1;

        metaCacheName = cfgs[0].getMetaCacheName();

        metaCache = ((IgniteKernal)grid).internalCache(metaCacheName);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        igfs.format();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(nodesCount());
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
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
        igfsCfg.setName("igfs");

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
            cacheCfg.setAffinityMapper(new IgfsGroupDataBlocksKeyMapper(128));
        }

        cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);

        return cacheCfg;
    }

    /** @return Test nodes count. */
    public int nodesCount() {
        return 1;
    }

    /** @return FS name. */
    public String igfsName() {
        return "igfs";
    }

    /** @throws Exception If failed. */
    public void testigfsEnabled() throws Exception {
        IgniteFileSystem igfs = grid(0).fileSystem(igfsName());

        assertNotNull(igfs);
    }

    /**
     * Test properties management in meta-cache.
     *
     * @throws Exception If failed.
     */
    public void testUpdateProperties() throws Exception {
        IgfsPath p = path("/tmp/my");

        igfs.mkdirs(p);

        Map<String, String> oldProps = igfs.info(p).properties();

        igfs.update(p, F.asMap("a", "1"));
        igfs.update(p, F.asMap("b", "2"));

        assertEquals("1", igfs.info(p).property("a"));
        assertEquals("2", igfs.info(p).property("b"));

        igfs.update(p, F.asMap("b", "3"));

        Map<String, String> expProps = new HashMap<>(oldProps);
        expProps.put("a", "1");
        expProps.put("b", "3");

        assertEquals("3", igfs.info(p).property("b"));
        assertEquals(expProps, igfs.info(p).properties());
        assertEquals("5", igfs.info(p).property("c", "5"));

        assertUpdatePropertiesFails(null, null, NullPointerException.class, "Ouch! Argument cannot be null");
        assertUpdatePropertiesFails(p, null, NullPointerException.class, "Ouch! Argument cannot be null");
        assertUpdatePropertiesFails(null, F.asMap("x", "9"), NullPointerException.class,
            "Ouch! Argument cannot be null");

        assertUpdatePropertiesFails(p, Collections.<String, String>emptyMap(), IllegalArgumentException.class,
            "Ouch! Argument is invalid");
    }

    /** @throws Exception If failed. */
    public void testCreate() throws Exception {
        IgfsPath path = path("/file");

        try (IgfsOutputStream os = igfs.create(path, false)) {
            assert os != null;

            IgfsFileImpl info = (IgfsFileImpl)igfs.info(path);

            for (int i = 0; i < nodesCount(); i++) {
                IgfsFileInfo fileInfo =
                    (IgfsFileInfo)grid(i).cachex(metaCacheName).localPeek(info.fileId(), ONHEAP_PEEK_MODES, null);

                assertNotNull(fileInfo);
                assertNotNull(fileInfo.listing());
            }
        }
        finally {
            igfs.delete(path("/"), true);
        }
    }

    /**
     * Test make directories.
     *
     * @throws Exception In case of any exception.
     */
    public void testMakeListDeleteDirs() throws Exception {
        assertListDir("/");

        igfs.mkdirs(path("/ab/cd/ef"));

        assertListDir("/", "ab");
        assertListDir("/ab", "cd");
        assertListDir("/ab/cd", "ef");

        igfs.mkdirs(path("/ab/ef"));
        igfs.mkdirs(path("/cd/ef"));
        igfs.mkdirs(path("/cd/gh"));
        igfs.mkdirs(path("/ef"));
        igfs.mkdirs(path("/ef/1"));
        igfs.mkdirs(path("/ef/2"));
        igfs.mkdirs(path("/ef/3"));

        assertListDir("/", "ef", "ab", "cd");
        assertListDir("/ab", "cd", "ef");
        assertListDir("/ab/cd", "ef");
        assertListDir("/ab/cd/ef");
        assertListDir("/cd", "ef", "gh");
        assertListDir("/cd/ef");
        assertListDir("/ef", "1", "2", "3");

        igfs.delete(path("/ef/2"), false);

        assertListDir("/", "ef", "ab", "cd");
        assertListDir("/ef", "1", "3");

        // Delete should return false for non-existing paths.
        assertFalse(igfs.delete(path("/ef/2"), false));

        assertListDir("/", "ef", "ab", "cd");
        assertListDir("/ef", "1", "3");

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                igfs.delete(path("/ef"), false);

                return null;
            }
        }, IgfsDirectoryNotEmptyException.class, null);

        assertListDir("/", "ef", "ab", "cd");
        assertListDir("/ef", "1", "3");

        igfs.delete(path("/ef"), true);

        assertListDir("/", "ab", "cd");
    }

    /**
     * Test make directories in multi-threaded environment.
     *
     * @throws Exception In case of any exception.
     */
    @SuppressWarnings("TooBroadScope")
    public void testMakeListDeleteDirsMultithreaded() throws Exception {
        assertListDir("/");

        final int max = 2 * 1000;
        final int threads = 50;
        final AtomicInteger cnt = new AtomicInteger();

        info("Create directories: " + max);

        GridTestUtils.runMultiThreaded(new Callable<Object>() {
            @Override public Object call() throws Exception {
                for (int cur = cnt.incrementAndGet(); cur < max; cur = cnt.incrementAndGet())
                    igfs.mkdirs(path(cur));

                return null;
            }
        }, threads, "grid-test-make-directories");

        info("Validate directories were created.");

        cnt.set(0); // Reset counter.

        GridTestUtils.runMultiThreaded(new Callable<Object>() {
            @Override public Object call() throws Exception {
                for (int cur = cnt.incrementAndGet(); cur < max; cur = cnt.incrementAndGet()) {
                    IgfsFile info = igfs.info(path(cur));

                    assertNotNull("Expects file exist: " + cur, info);
                    assertTrue("Expects file is a directory: " + cur, info.isDirectory());
                }

                return null;
            }
        }, threads, "grid-test-check-directories-exist");

        info("Validate directories removing.");

        cnt.set(0); // Reset counter.

        GridTestUtils.runMultiThreaded(new Callable<Object>() {
            @Override public Object call() throws Exception {
                for (int cur = cnt.incrementAndGet(); cur < max; cur = cnt.incrementAndGet())
                    igfs.delete(path(cur), true);

                return null;
            }
        }, threads, "grid-test-delete-directories");
    }

    /** @throws Exception If failed. */
    public void testBasicOps() throws Exception {
        // Create directories.
        igfs.mkdirs(path("/A/B1/C1"));

        for (Object key : metaCache.keySet())
            info("Entry in cache [key=" + key + ", val=" + metaCache.get(key) + ']');

        igfs.mkdirs(path("/A/B1/C2"));
        igfs.mkdirs(path("/A/B1/C3"));
        igfs.mkdirs(path("/A/B2/C1"));
        igfs.mkdirs(path("/A/B2/C2"));

        igfs.mkdirs(path("/A1/B1/C1"));
        igfs.mkdirs(path("/A1/B1/C2"));
        igfs.mkdirs(path("/A1/B1/C3"));
        igfs.mkdirs(path("/A2/B2/C1"));
        igfs.mkdirs(path("/A2/B2/C2"));

        for (Object key : metaCache.keySet())
            info("Entry in cache [key=" + key + ", val=" + metaCache.get(key) + ']');

        // Check existence.
        assert igfs.exists(path("/A/B1/C1"));

        // List items.
        Collection<IgfsPath> paths = igfs.listPaths(path("/"));

        assert paths.size() == 3 : "Unexpected paths: " + paths;

        paths = igfs.listPaths(path("/A"));

        assert paths.size() == 2 : "Unexpected paths: " + paths;

        paths = igfs.listPaths(path("/A/B1"));

        assert paths.size() == 3 : "Unexpected paths: " + paths;

        // Delete.
        GridTestUtils.assertThrowsInherited(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                igfs.delete(path("/"), false);

                return null;
            }
        }, IgfsException.class, null);

        igfs.delete(path("/A1/B1/C1"), false);
        assertNull(igfs.info(path("/A1/B1/C1")));

        igfs.delete(path("/A1/B1/C2"), false);
        assertNull(igfs.info(path("/A1/B1/C2")));

        igfs.delete(path("/A1/B1/C3"), false);
        assertNull(igfs.info(path("/A1/B1/C3")));

        assertEquals(Collections.<IgfsPath>emptyList(), igfs.listPaths(path("/A1/B1")));

        igfs.delete(path("/A2/B2"), true);
        assertNull(igfs.info(path("/A2/B2")));

        assertEquals(Collections.<IgfsPath>emptyList(), igfs.listPaths(path("/A2")));

        assertEquals(Arrays.asList(path("/A"), path("/A1"), path("/A2")), sorted(igfs.listPaths(path("/"))));

        GridTestUtils.assertThrowsInherited(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                igfs.delete(path("/"), false);

                return null;
            }
        }, IgfsException.class, null);
        assertEquals(Arrays.asList(path("/A"), path("/A1"), path("/A2")), sorted(igfs.listPaths(path("/"))));

        igfs.delete(path("/"), true);
        assertEquals(Collections.<IgfsPath>emptyList(), igfs.listPaths(path("/")));

        igfs.delete(path("/"), false);
        assertEquals(Collections.<IgfsPath>emptyList(), igfs.listPaths(path("/")));

        for (Cache.Entry<Object, Object> e : metaCache)
            info("Entry in cache [key=" + e.getKey() + ", val=" + e.getValue() + ']');
    }

    /**
     * Ensure correct size calculation.
     *
     * @throws Exception If failed.
     */
    public void testSize() throws Exception {
        IgfsPath dir1 = path("/dir1");
        IgfsPath subDir1 = path("/dir1/subdir1");
        IgfsPath dir2 = path("/dir2");

        IgfsPath fileDir1 = path("/dir1/file");
        IgfsPath fileSubdir1 = path("/dir1/subdir1/file");
        IgfsPath fileDir2 = path("/dir2/file");

        IgfsOutputStream os = igfs.create(fileDir1, false);
        os.write(new byte[1000]);
        os.close();

        os = igfs.create(fileSubdir1, false);
        os.write(new byte[2000]);
        os.close();

        os = igfs.create(fileDir2, false);
        os.write(new byte[4000]);
        os.close();

        assert igfs.size(fileDir1) == 1000;
        assert igfs.size(fileSubdir1) == 2000;
        assert igfs.size(fileDir2) == 4000;

        assert igfs.size(dir1) == 3000;
        assert igfs.size(subDir1) == 2000;

        assert igfs.size(dir2) == 4000;
    }

    /**
     * Convert collection into sorted list.
     *
     * @param col Unsorted collection.
     * @return Sorted collection.
     */
    private <T extends Comparable<T>> List<T> sorted(Collection<T> col) {
        List<T> list = new ArrayList<>(col);

        Collections.sort(list);

        return list;
    }

    /** @throws Exception If failed. */
    public void testRename() throws Exception {
        // Create directories.
        igfs.mkdirs(path("/A/B1/C1"));

        for (Object key : metaCache.keySet())
            info("Entry in cache [key=" + key + ", val=" + metaCache.get(key) + ']');

        // Move under itself.
        GridTestUtils.assertThrowsInherited(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                igfs.rename(path("/A/B1/C1"), path("/A/B1/C1/C2"));

                return null;
            }
        }, IgfsException.class, null);

        // Move under itself.
        GridTestUtils.assertThrowsInherited(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                igfs.rename(path("/A/B1/C1"), path("/A/B1/C1/D/C2"));

                return null;
            }
        }, IgfsException.class, null);

        // Move under itself.
        GridTestUtils.assertThrowsInherited(log, new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                igfs.rename(path("/A/B1/C1"), path("/A/B1/C1/D/E/C2"));

                return null;
            }
        }, IgfsException.class, null);

        ///
        // F6 > Enter > Tab x N times
        // "I like to move it, move it..."
        //

        Collection<IgniteBiTuple<String, String>> chain = Arrays.asList(
            F.t("/A/B1/C1", "/A/B1/C2"),
            F.t("/A/B1", "/A/B2"),
            F.t("/A", "/Q"),
            //F.t("/Q/B2/C2", "/C3"),
            F.t("/Q/B2/C2", "/Q/B2/C1"),
            F.t("/Q/B2", "/Q/B1"),
            F.t("/Q", "/A"),
            //F.t("/C3", "/A/B1/C1")
            F.t("/A/B1/C1", "/"),
            F.t("/C1", "/A/B1")
        );

        final IgfsPath root = path("/");

        for (IgniteBiTuple<String, String> e : chain) {
            final IgfsPath p1 = path(e.get1());
            final IgfsPath p2 = path(e.get2());

            assertTrue("Entry: " + e, igfs.exists(p1));
            igfs.rename(p1, p2);
            assertFalse("Entry: " + e, igfs.exists(p1));
            assertTrue("Entry: " + e, igfs.exists(p2));

            // Test root rename.
            GridTestUtils.assertThrowsInherited(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    igfs.rename(root, p1);

                    return null;
                }
            }, IgfsException.class, null);

            // Test root rename.
            GridTestUtils.assertThrowsInherited(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    igfs.rename(p1, root);

                    return null;
                }
            }, IgfsException.class, null);

            // Test root rename.
            if (!root.equals(p2)) {
                GridTestUtils.assertThrowsInherited(log, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        igfs.rename(root, p2);

                        return null;
                    }
                }, IgfsException.class, null);
            }

            // Test same rename.
            igfs.rename(p1, p1);
            igfs.rename(p2, p2);
        }

        // List items.
        assertEquals(Arrays.asList(path("/A")), sorted(igfs.listPaths(root)));
        assertEquals(Arrays.asList(path("/A/B1")), sorted(igfs.listPaths(path("/A"))));
        assertEquals(Arrays.asList(path("/A/B1/C1")), sorted(igfs.listPaths(path("/A/B1"))));

        String text = "Test long number: " + rnd.nextLong();

        // Create file.
        assertEquals(text, create("/A/a", false, text));

        // Validate renamed during reading.

        try (IgfsInputStream in0 = igfs.open(path("/A/a"))) {
            // Rename file.
            igfs.rename(path("/A/a"), path("/b"));

            assertEquals(text, IOUtils.toString(in0, UTF_8));
        }

        // Validate after renamed.
        assertOpenFails("/A/a", "File not found");
        assertEquals(text, read("/b"));

        // Cleanup.
        igfs.delete(root, true);

        assertEquals(Collections.<IgfsPath>emptyList(), igfs.listPaths(root));
    }

    /**
     * @param path Path.
     * @return IGFS path.
     */
    private IgfsPath path(String path) {
        assert path != null;

        return new IgfsPath(path);
    }

    /**
     * @param i Path index.
     * @return IGFS path.
     */
    private IgfsPath path(long i) {
        //return path(String.format("/%d", i));
        return path(String.format("/%d/q/%d/%d", i % 10, (i / 10) % 10, i));
    }

    /** @throws Exception If failed. */
    public void testCreateOpenAppend() throws Exception {
        // Error - path points to root directory.
        assertCreateFails("/", false, "Failed to resolve parent directory");

        // Create directories.
        igfs.mkdirs(path("/A/B1/C1"));

        // Error - path points to directory.
        for (String path : Arrays.asList("/A", "/A/B1", "/A/B1/C1")) {
            assertCreateFails(path, false, "Failed to create file (file already exists)");
            assertCreateFails(path, true, "Failed to create file (path points to a directory)");
            assertAppendFails(path, false, "Failed to open file (not a file)");
            assertAppendFails(path, true, "Failed to open file (not a file)");
            assertOpenFails(path, "Failed to open file (not a file)");
        }

        String text1 = "Test long number #1: " + rnd.nextLong();
        String text2 = "Test long number #2: " + rnd.nextLong();

        // Error - parent does not exist.
        for (String path : Arrays.asList("/A/a", "/A/B1/a", "/A/B1/C1/a")) {
            // Error - file doesn't exist.
            assertOpenFails(path, "File not found");
            assertAppendFails(path, false, "File not found");

            // Create new and write.
            assertEquals(text1, create(path, false, text1));

            // Error - file already exists.
            assertCreateFails(path, false, "Failed to create file (file already exists)");

            // Overwrite existent.
            assertEquals(text2, create(path, true, text2));

            // Append text.
            assertEquals(text2 + text1, append(path, false, text1));

            // Append text.
            assertEquals(text2 + text1 + text2, append(path, true, text2));

            // Delete this file.
            igfs.delete(path(path), true);

            // Error - file doesn't exist.
            assertOpenFails(path, "File not found");
            assertAppendFails(path, false, "File not found");

            // Create with append.
            assertEquals(text1, append(path, true, text1));

            // Append.
            for (String full = text1, cur = ""; full.length() < 10000; cur = ", long=" + rnd.nextLong())
                assertEquals(full += cur, append(path, rnd.nextBoolean(), cur));

            igfs.delete(path(path), false);
        }
    }

    /** @throws Exception If failed. */
    @SuppressWarnings("BusyWait")
    public void testDeleteCacheConsistency() throws Exception {
        IgfsPath path = new IgfsPath("/someFile");

        try (IgfsOutputStream out = igfs.create(path, true)) {
            out.write(new byte[10 * 1024 * 1024]);
        }

        IgniteUuid fileId = U.field(igfs.info(path), "fileId");

        GridCacheAdapter<IgniteUuid, IgfsFileInfo> metaCache = ((IgniteKernal)grid(0)).internalCache(META_CACHE_NAME);
        GridCacheAdapter<IgfsBlockKey, byte[]> dataCache = ((IgniteKernal)grid(0)).internalCache(DATA_CACHE_NAME);

        IgfsFileInfo info = metaCache.get(fileId);

        assertNotNull(info);
        assertTrue(info.isFile());
        assertNotNull(metaCache.get(info.id()));

        IgfsDataManager dataMgr = ((IgfsEx)igfs).context().data();

        for (int i = 0; i < info.blocksCount(); i++)
            assertNotNull(dataCache.get(dataMgr.blockKey(i, info)));

        igfs.delete(path, true);

        for (int i = 0; i < 25; i++) {
            if (metaCache.get(info.id()) == null)
                break;

            U.sleep(100);
        }

        assertNull(metaCache.get(info.id()));

        for (int i = 0; i < 10; i++) {
            boolean doBreak = true;

            for (int j = 0; j < info.blocksCount(); j++) {
                if (dataCache.get(dataMgr.blockKey(i, info)) != null) {
                    doBreak = false;

                    break;
                }
            }

            if (doBreak)
                break;
            else
                Thread.sleep(100);
        }

        for (int i = 0; i < info.blocksCount(); i++)
            assertNull(dataCache.get(new IgfsBlockKey(info.id(), null, false, i)));
    }

    /** @throws Exception If failed. */
    public void testCreateAppendLongData1() throws Exception {
        checkCreateAppendLongData(123, 1024, 100);
    }

    /** @throws Exception If failed. */
    public void testCreateAppendLongData2() throws Exception {
        checkCreateAppendLongData(123 + 1024, 1024, 100);
    }

    /** @throws Exception If failed. */
    public void testCreateAppendLongData3() throws Exception {
        checkCreateAppendLongData(123, 1024, 1000);
    }

    /** @throws Exception If failed. */
    public void testCreateAppendLongData4() throws Exception {
        checkCreateAppendLongData(123 + 1024, 1024, 1000);
    }

    /**
     * Test format operation on non-empty file system.
     *
     * @throws Exception If failed.
     */
    public void testFormatNonEmpty() throws Exception {
        String dirPath = "/A/B/C";

        igfs.mkdirs(path(dirPath));

        String filePath = "/someFile";

        create(filePath, false, "Some text.");

        igfs.format();

        assert !igfs.exists(path(dirPath));
        assert !igfs.exists(path(filePath));

        assert grid(0).cachex(igfs.configuration().getMetaCacheName()).size() == 2; // ROOT + TRASH.
    }

    /**
     * Test format operation on empty file system.
     *
     * @throws Exception If failed.
     */
    public void testFormatEmpty() throws Exception {
        igfs.format();
    }

    /**
     * @param chunkSize Chunk size.
     * @param bufSize Buffer size.
     * @param cnt Count.
     * @throws Exception If failed.
     */
    private void checkCreateAppendLongData(int chunkSize, int bufSize, int cnt) throws Exception {
        IgfsPath path = new IgfsPath("/someFile");

        byte[] buf = new byte[chunkSize];

        for (int i = 0; i < buf.length; i++)
            buf[i] = (byte)(i * i);

        IgfsOutputStream os = igfs.create(path, bufSize, true, null, 0, 1024, null);

        try {
            for (int i = 0; i < cnt; i++)
                os.write(buf);

            os.flush();
        }
        finally {
            os.close();
        }

        os = igfs.append(path, chunkSize, false, null);

        try {
            for (int i = 0; i < cnt; i++)
                os.write(buf);

            os.flush();
        }
        finally {
            os.close();
        }

        byte[] readBuf = new byte[chunkSize];

        try (IgfsInputStream in = igfs.open(path)) {
            long pos = 0;

            for (int k = 0; k < 2 * cnt; k++) {
                in.readFully(pos, readBuf);

                for (int i = 0; i < readBuf.length; i++)
                    assertEquals(buf[i], readBuf[i]);

                pos += readBuf.length;
            }
        }
    }

    /**
     * Create file and write specified text to.
     *
     * @param path File path to create.
     * @param overwrite Overwrite file if it already exists.
     * @param text Text to write into file.
     * @return Content of this file.
     * @throws IgniteCheckedException In case of error.
     */
    private String create(String path, boolean overwrite, String text) throws Exception {

        try (IgfsOutputStream out = igfs.create(path(path), overwrite)) {
            IOUtils.write(text, out, UTF_8);
        }

        assertNotNull(igfs.info(path(path)));

        return read(path);
    }

    /**
     * Appent text to the file.
     *
     * @param path File path to create.
     * @param create Create file if it doesn't exist yet.
     * @param text Text to append to file.
     * @return Content of this file.
     * @throws IgniteCheckedException In case of error.
     */
    private String append(String path, boolean create, String text) throws Exception {

        try (IgfsOutputStream out = igfs.append(path(path), create)) {
            IOUtils.write(text, out, UTF_8);
        }

        assertNotNull(igfs.info(path(path)));

        return read(path);
    }

    /**
     * Read content of the file.
     *
     * @param path File path to read.
     * @return Content of this file.
     * @throws IgniteCheckedException In case of error.
     */
    private String read(String path) throws Exception {

        try (IgfsInputStream in = igfs.open(path(path))) {
            return IOUtils.toString(in, UTF_8);
        }
    }

    /**
     * Test expected failures for 'update properties' operation.
     *
     * @param path Path to the file.
     * @param props File properties to set.
     * @param msg Failure message if expected exception was not thrown.
     */
    private void assertUpdatePropertiesFails(@Nullable final IgfsPath path,
        @Nullable final Map<String, String> props,
        Class<? extends Throwable> cls, @Nullable String msg) {
        GridTestUtils.assertThrows(log, new Callable() {
            @Override public Object call() throws Exception {
                return igfs.update(path, props);
            }
        }, cls, msg);
    }

    /**
     * Test expected failures for 'create' operation.
     *
     * @param path File path to create.
     * @param overwrite Overwrite file if it already exists. Note: you cannot overwrite an existent directory.
     * @param msg Failure message if expected exception was not thrown.
     */
    private void assertCreateFails(final String path, final boolean overwrite, @Nullable String msg) {
        GridTestUtils.assertThrowsInherited(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                igfs.create(path(path), overwrite);

                return false;
            }
        }, IgfsException.class, msg);
    }

    /**
     * Test expected failures for 'append' operation.
     *
     * @param path File path to append.
     * @param create Create file if it doesn't exist yet.
     * @param msg Failure message if expected exception was not thrown.
     */
    private void assertAppendFails(final String path, final boolean create, @Nullable String msg) {
        GridTestUtils.assertThrowsInherited(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                igfs.append(path(path), create);

                return false;
            }
        }, IgfsException.class, msg);
    }

    /**
     * Test expected failures for 'open' operation.
     *
     * @param path File path to read.
     * @param msg Failure message if expected exception was not thrown.
     */
    private void assertOpenFails(final String path, @Nullable String msg) {
        GridTestUtils.assertThrowsInherited(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                igfs.open(path(path));

                return false;
            }
        }, IgniteException.class, msg);
    }

    /**
     * Validate directory listing.
     *
     * @param path Directory path to validate listing for.
     * @param item List of directory items.
     */
    private void assertListDir(String path, String... item) {
        Collection<IgfsFile> files = igfs.listFiles(new IgfsPath(path));

        List<String> names = new ArrayList<>(item.length);

        for (IgfsFile file : files)
            names.add(file.path().name());

        Arrays.sort(item);
        Collections.sort(names);

        assertEquals(Arrays.asList(item), names);
    }
}