/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.ggfs;

import org.apache.commons.io.*;
import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;
import org.jetbrains.annotations.*;

import java.security.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static java.nio.charset.StandardCharsets.*;
import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;

/**
 * Tests for {@link GridGgfsProcessor}.
 */
public class GridGgfsProcessorSelfTest extends GridGgfsCommonAbstractTest {
    /** Test IP finder. */
    private static final GridTcpDiscoveryIpFinder IP_FINDER = new GridTcpDiscoveryVmIpFinder(true);

    /** Meta-information cache name. */
    private static final String META_CACHE_NAME = "replicated";

    /** Data cache name. */
    public static final String DATA_CACHE_NAME = "data";

    /** Random numbers generator. */
    protected final SecureRandom rnd = new SecureRandom();

    /** File system. */
    protected IgniteFs ggfs;

    /** Meta cache. */
    private GridCache<Object, Object> metaCache;

    /** Meta cache name. */
    private String metaCacheName;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        GridEx grid = grid(0);

        ggfs = grid.fileSystem(ggfsName());

        IgniteFsConfiguration[] cfgs = grid.configuration().getGgfsConfiguration();

        assert cfgs.length == 1;

        metaCacheName = cfgs[0].getMetaCacheName();

        metaCache = grid.cachex(metaCacheName);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        ggfs.format();
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

        GridTcpDiscoverySpi discoSpi = new GridTcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        IgniteFsConfiguration ggfsCfg = new IgniteFsConfiguration();

        ggfsCfg.setMetaCacheName(META_CACHE_NAME);
        ggfsCfg.setDataCacheName(DATA_CACHE_NAME);
        ggfsCfg.setName("ggfs");

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
            cacheCfg.setAffinityMapper(new IgniteFsGroupDataBlocksKeyMapper(128));
        }

        cacheCfg.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);
        cacheCfg.setQueryIndexEnabled(false);

        return cacheCfg;
    }

    /** @return Test nodes count. */
    public int nodesCount() {
        return 1;
    }

    /** @return FS name. */
    public String ggfsName() {
        return "ggfs";
    }

    /** @throws Exception If failed. */
    public void testGgfsEnabled() throws Exception {
        IgniteFs ggfs = grid(0).fileSystem(ggfsName());

        assertNotNull(ggfs);
    }

    /**
     * Test properties management in meta-cache.
     *
     * @throws Exception If failed.
     */
    public void testUpdateProperties() throws Exception {
        IgniteFsPath p = path("/tmp/my");

        ggfs.mkdirs(p);

        Map<String, String> oldProps = ggfs.info(p).properties();

        ggfs.update(p, F.asMap("a", "1"));
        ggfs.update(p, F.asMap("b", "2"));

        assertEquals("1", ggfs.info(p).property("a"));
        assertEquals("2", ggfs.info(p).property("b"));

        ggfs.update(p, F.asMap("b", "3"));

        Map<String, String> expProps = new HashMap<>(oldProps);
        expProps.put("a", "1");
        expProps.put("b", "3");

        assertEquals("3", ggfs.info(p).property("b"));
        assertEquals(expProps, ggfs.info(p).properties());
        assertEquals("5", ggfs.info(p).property("c", "5"));

        assertUpdatePropertiesFails(null, null, NullPointerException.class, "Ouch! Argument cannot be null");
        assertUpdatePropertiesFails(p, null, NullPointerException.class, "Ouch! Argument cannot be null");
        assertUpdatePropertiesFails(null, F.asMap("x", "9"), NullPointerException.class,
            "Ouch! Argument cannot be null");

        assertUpdatePropertiesFails(p, Collections.<String, String>emptyMap(), IllegalArgumentException.class,
            "Ouch! Argument is invalid");
    }

    /** @throws Exception If failed. */
    public void testCreate() throws Exception {
        IgniteFsPath path = path("/file");

        try (GridGgfsOutputStream os = ggfs.create(path, false)) {
            assert os != null;

            IgniteFsFileImpl info = (IgniteFsFileImpl)ggfs.info(path);

            for (int i = 0; i < nodesCount(); i++) {
                GridGgfsFileInfo fileInfo = (GridGgfsFileInfo)grid(i).cachex(metaCacheName).peek(info.fileId());

                assertNotNull(fileInfo);
                assertNotNull(fileInfo.listing());
            }
        }
        finally {
            ggfs.delete(path("/"), true);
        }
    }

    /**
     * Test make directories.
     *
     * @throws Exception In case of any exception.
     */
    public void testMakeListDeleteDirs() throws Exception {
        assertListDir("/");

        ggfs.mkdirs(path("/ab/cd/ef"));

        assertListDir("/", "ab");
        assertListDir("/ab", "cd");
        assertListDir("/ab/cd", "ef");

        ggfs.mkdirs(path("/ab/ef"));
        ggfs.mkdirs(path("/cd/ef"));
        ggfs.mkdirs(path("/cd/gh"));
        ggfs.mkdirs(path("/ef"));
        ggfs.mkdirs(path("/ef/1"));
        ggfs.mkdirs(path("/ef/2"));
        ggfs.mkdirs(path("/ef/3"));

        assertListDir("/", "ef", "ab", "cd");
        assertListDir("/ab", "cd", "ef");
        assertListDir("/ab/cd", "ef");
        assertListDir("/ab/cd/ef");
        assertListDir("/cd", "ef", "gh");
        assertListDir("/cd/ef");
        assertListDir("/ef", "1", "2", "3");

        ggfs.delete(path("/ef/2"), false);

        assertListDir("/", "ef", "ab", "cd");
        assertListDir("/ef", "1", "3");

        // Delete should return false for non-existing paths.
        assertFalse(ggfs.delete(path("/ef/2"), false));

        assertListDir("/", "ef", "ab", "cd");
        assertListDir("/ef", "1", "3");

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                ggfs.delete(path("/ef"), false);

                return null;
            }
        }, GridGgfsDirectoryNotEmptyException.class, null);

        assertListDir("/", "ef", "ab", "cd");
        assertListDir("/ef", "1", "3");

        ggfs.delete(path("/ef"), true);

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
                    ggfs.mkdirs(path(cur));

                return null;
            }
        }, threads, "grid-test-make-directories");

        info("Validate directories were created.");

        cnt.set(0); // Reset counter.

        GridTestUtils.runMultiThreaded(new Callable<Object>() {
            @Override public Object call() throws Exception {
                for (int cur = cnt.incrementAndGet(); cur < max; cur = cnt.incrementAndGet()) {
                    IgniteFsFile info = ggfs.info(path(cur));

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
                    ggfs.delete(path(cur), true);

                return null;
            }
        }, threads, "grid-test-delete-directories");
    }

    /** @throws Exception If failed. */
    public void testBasicOps() throws Exception {
        // Create directories.
        ggfs.mkdirs(path("/A/B1/C1"));

        for (Object key : metaCache.keySet())
            info("Entry in cache [key=" + key + ", val=" + metaCache.get(key) + ']');

        ggfs.mkdirs(path("/A/B1/C2"));
        ggfs.mkdirs(path("/A/B1/C3"));
        ggfs.mkdirs(path("/A/B2/C1"));
        ggfs.mkdirs(path("/A/B2/C2"));

        ggfs.mkdirs(path("/A1/B1/C1"));
        ggfs.mkdirs(path("/A1/B1/C2"));
        ggfs.mkdirs(path("/A1/B1/C3"));
        ggfs.mkdirs(path("/A2/B2/C1"));
        ggfs.mkdirs(path("/A2/B2/C2"));

        for (Object key : metaCache.keySet())
            info("Entry in cache [key=" + key + ", val=" + metaCache.get(key) + ']');

        // Check existence.
        assert ggfs.exists(path("/A/B1/C1"));

        // List items.
        Collection<IgniteFsPath> paths = ggfs.listPaths(path("/"));

        assert paths.size() == 3 : "Unexpected paths: " + paths;

        paths = ggfs.listPaths(path("/A"));

        assert paths.size() == 2 : "Unexpected paths: " + paths;

        paths = ggfs.listPaths(path("/A/B1"));

        assert paths.size() == 3 : "Unexpected paths: " + paths;

        // Delete.
        GridTestUtils.assertThrowsInherited(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                ggfs.delete(path("/"), false);

                return null;
            }
        }, IgniteFsException.class, null);

        ggfs.delete(path("/A1/B1/C1"), false);
        assertNull(ggfs.info(path("/A1/B1/C1")));

        ggfs.delete(path("/A1/B1/C2"), false);
        assertNull(ggfs.info(path("/A1/B1/C2")));

        ggfs.delete(path("/A1/B1/C3"), false);
        assertNull(ggfs.info(path("/A1/B1/C3")));

        assertEquals(Collections.<IgniteFsPath>emptyList(), ggfs.listPaths(path("/A1/B1")));

        ggfs.delete(path("/A2/B2"), true);
        assertNull(ggfs.info(path("/A2/B2")));

        assertEquals(Collections.<IgniteFsPath>emptyList(), ggfs.listPaths(path("/A2")));

        assertEquals(Arrays.asList(path("/A"), path("/A1"), path("/A2")), sorted(ggfs.listPaths(path("/"))));

        GridTestUtils.assertThrowsInherited(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                ggfs.delete(path("/"), false);

                return null;
            }
        }, IgniteFsException.class, null);
        assertEquals(Arrays.asList(path("/A"), path("/A1"), path("/A2")), sorted(ggfs.listPaths(path("/"))));

        ggfs.delete(path("/"), true);
        assertEquals(Collections.<IgniteFsPath>emptyList(), ggfs.listPaths(path("/")));

        ggfs.delete(path("/"), false);
        assertEquals(Collections.<IgniteFsPath>emptyList(), ggfs.listPaths(path("/")));

        for (GridCacheEntry<Object, Object> e : metaCache)
            info("Entry in cache [key=" + e.getKey() + ", val=" + e.getValue() + ']');
    }

    /**
     * Ensure correct size calculation.
     *
     * @throws Exception If failed.
     */
    public void testSize() throws Exception {
        IgniteFsPath dir1 = path("/dir1");
        IgniteFsPath subDir1 = path("/dir1/subdir1");
        IgniteFsPath dir2 = path("/dir2");

        IgniteFsPath fileDir1 = path("/dir1/file");
        IgniteFsPath fileSubdir1 = path("/dir1/subdir1/file");
        IgniteFsPath fileDir2 = path("/dir2/file");

        GridGgfsOutputStream os = ggfs.create(fileDir1, false);
        os.write(new byte[1000]);
        os.close();

        os = ggfs.create(fileSubdir1, false);
        os.write(new byte[2000]);
        os.close();

        os = ggfs.create(fileDir2, false);
        os.write(new byte[4000]);
        os.close();

        assert ggfs.size(fileDir1) == 1000;
        assert ggfs.size(fileSubdir1) == 2000;
        assert ggfs.size(fileDir2) == 4000;

        assert ggfs.size(dir1) == 3000;
        assert ggfs.size(subDir1) == 2000;

        assert ggfs.size(dir2) == 4000;
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
        ggfs.mkdirs(path("/A/B1/C1"));

        for (Object key : metaCache.keySet())
            info("Entry in cache [key=" + key + ", val=" + metaCache.get(key) + ']');

        // Move under itself.
        GridTestUtils.assertThrowsInherited(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                ggfs.rename(path("/A/B1/C1"), path("/A/B1/C1/C2"));

                return null;
            }
        }, IgniteFsException.class, null);

        // Move under itself.
        GridTestUtils.assertThrowsInherited(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                ggfs.rename(path("/A/B1/C1"), path("/A/B1/C1/D/C2"));

                return null;
            }
        }, IgniteFsException.class, null);

        // Move under itself.
        GridTestUtils.assertThrowsInherited(log, new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                ggfs.rename(path("/A/B1/C1"), path("/A/B1/C1/D/E/C2"));

                return null;
            }
        }, IgniteFsException.class, null);

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

        final IgniteFsPath root = path("/");

        for (IgniteBiTuple<String, String> e : chain) {
            final IgniteFsPath p1 = path(e.get1());
            final IgniteFsPath p2 = path(e.get2());

            assertTrue("Entry: " + e, ggfs.exists(p1));
            ggfs.rename(p1, p2);
            assertFalse("Entry: " + e, ggfs.exists(p1));
            assertTrue("Entry: " + e, ggfs.exists(p2));

            // Test root rename.
            GridTestUtils.assertThrowsInherited(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    ggfs.rename(root, p1);

                    return null;
                }
            }, IgniteFsException.class, null);

            // Test root rename.
            GridTestUtils.assertThrowsInherited(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    ggfs.rename(p1, root);

                    return null;
                }
            }, IgniteFsException.class, null);

            // Test root rename.
            if (!root.equals(p2)) {
                GridTestUtils.assertThrowsInherited(log, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        ggfs.rename(root, p2);

                        return null;
                    }
                }, IgniteFsException.class, null);
            }

            // Test same rename.
            ggfs.rename(p1, p1);
            ggfs.rename(p2, p2);
        }

        // List items.
        assertEquals(Arrays.asList(path("/A")), sorted(ggfs.listPaths(root)));
        assertEquals(Arrays.asList(path("/A/B1")), sorted(ggfs.listPaths(path("/A"))));
        assertEquals(Arrays.asList(path("/A/B1/C1")), sorted(ggfs.listPaths(path("/A/B1"))));

        String text = "Test long number: " + rnd.nextLong();

        // Create file.
        assertEquals(text, create("/A/a", false, text));

        // Validate renamed during reading.

        try (GridGgfsInputStream in0 = ggfs.open(path("/A/a"))) {
            // Rename file.
            ggfs.rename(path("/A/a"), path("/b"));

            assertEquals(text, IOUtils.toString(in0, UTF_8));
        }

        // Validate after renamed.
        assertOpenFails("/A/a", "File not found");
        assertEquals(text, read("/b"));

        // Cleanup.
        ggfs.delete(root, true);

        assertEquals(Collections.<IgniteFsPath>emptyList(), ggfs.listPaths(root));
    }

    /**
     * @param path Path.
     * @return GGFS path.
     */
    private IgniteFsPath path(String path) {
        assert path != null;

        return new IgniteFsPath(path);
    }

    /**
     * @param i Path index.
     * @return GGFS path.
     */
    private IgniteFsPath path(long i) {
        //return path(String.format("/%d", i));
        return path(String.format("/%d/q/%d/%d", i % 10, (i / 10) % 10, i));
    }

    /** @throws Exception If failed. */
    public void testCreateOpenAppend() throws Exception {
        // Error - path points to root directory.
        assertCreateFails("/", false, "Failed to resolve parent directory");

        // Create directories.
        ggfs.mkdirs(path("/A/B1/C1"));

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
            ggfs.delete(path(path), true);

            // Error - file doesn't exist.
            assertOpenFails(path, "File not found");
            assertAppendFails(path, false, "File not found");

            // Create with append.
            assertEquals(text1, append(path, true, text1));

            // Append.
            for (String full = text1, cur = ""; full.length() < 10000; cur = ", long=" + rnd.nextLong())
                assertEquals(full += cur, append(path, rnd.nextBoolean(), cur));

            ggfs.delete(path(path), false);
        }
    }

    /** @throws Exception If failed. */
    @SuppressWarnings("BusyWait")
    public void testDeleteCacheConsistency() throws Exception {
        IgniteFsPath path = new IgniteFsPath("/someFile");

        try (GridGgfsOutputStream out = ggfs.create(path, true)) {
            out.write(new byte[10 * 1024 * 1024]);
        }

        IgniteUuid fileId = U.field(ggfs.info(path), "fileId");

        GridCache<IgniteUuid, GridGgfsFileInfo> metaCache = grid(0).cachex(META_CACHE_NAME);
        GridCache<GridGgfsBlockKey, byte[]> dataCache = grid(0).cachex(DATA_CACHE_NAME);

        GridGgfsFileInfo info = metaCache.get(fileId);

        assertNotNull(info);
        assertTrue(info.isFile());
        assertNotNull(metaCache.get(info.id()));

        GridGgfsDataManager dataMgr = ((GridGgfsEx)ggfs).context().data();

        for (int i = 0; i < info.blocksCount(); i++)
            assertNotNull(dataCache.get(dataMgr.blockKey(i, info)));

        ggfs.delete(path, true);

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
            assertNull(dataCache.get(new GridGgfsBlockKey(info.id(), null, false, i)));
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

        ggfs.mkdirs(path(dirPath));

        String filePath = "/someFile";

        create(filePath, false, "Some text.");

        ggfs.format();

        assert !ggfs.exists(path(dirPath));
        assert !ggfs.exists(path(filePath));

        assert grid(0).cachex(ggfs.configuration().getMetaCacheName()).size() == 2; // ROOT + TRASH.
    }

    /**
     * Test format operation on empty file system.
     *
     * @throws Exception If failed.
     */
    public void testFormatEmpty() throws Exception {
        ggfs.format();
    }

    /**
     * @param chunkSize Chunk size.
     * @param bufSize Buffer size.
     * @param cnt Count.
     * @throws Exception If failed.
     */
    private void checkCreateAppendLongData(int chunkSize, int bufSize, int cnt) throws Exception {
        IgniteFsPath path = new IgniteFsPath("/someFile");

        byte[] buf = new byte[chunkSize];

        for (int i = 0; i < buf.length; i++)
            buf[i] = (byte)(i * i);

        GridGgfsOutputStream os = ggfs.create(path, bufSize, true, null, 0, 1024, null);

        try {
            for (int i = 0; i < cnt; i++)
                os.write(buf);

            os.flush();
        }
        finally {
            os.close();
        }

        os = ggfs.append(path, chunkSize, false, null);

        try {
            for (int i = 0; i < cnt; i++)
                os.write(buf);

            os.flush();
        }
        finally {
            os.close();
        }

        byte[] readBuf = new byte[chunkSize];

        try (GridGgfsInputStream in = ggfs.open(path)) {
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
     * @throws GridException In case of error.
     */
    private String create(String path, boolean overwrite, String text) throws Exception {

        try (GridGgfsOutputStream out = ggfs.create(path(path), overwrite)) {
            IOUtils.write(text, out, UTF_8);
        }

        assertNotNull(ggfs.info(path(path)));

        return read(path);
    }

    /**
     * Appent text to the file.
     *
     * @param path File path to create.
     * @param create Create file if it doesn't exist yet.
     * @param text Text to append to file.
     * @return Content of this file.
     * @throws GridException In case of error.
     */
    private String append(String path, boolean create, String text) throws Exception {

        try (GridGgfsOutputStream out = ggfs.append(path(path), create)) {
            IOUtils.write(text, out, UTF_8);
        }

        assertNotNull(ggfs.info(path(path)));

        return read(path);
    }

    /**
     * Read content of the file.
     *
     * @param path File path to read.
     * @return Content of this file.
     * @throws GridException In case of error.
     */
    private String read(String path) throws Exception {

        try (GridGgfsInputStream in = ggfs.open(path(path))) {
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
    private void assertUpdatePropertiesFails(@Nullable final IgniteFsPath path,
        @Nullable final Map<String, String> props,
        Class<? extends Throwable> cls, @Nullable String msg) {
        GridTestUtils.assertThrows(log, new Callable() {
            @Override public Object call() throws Exception {
                return ggfs.update(path, props);
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
                ggfs.create(path(path), overwrite);

                return false;
            }
        }, IgniteFsException.class, msg);
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
                ggfs.append(path(path), create);

                return false;
            }
        }, IgniteFsException.class, msg);
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
                ggfs.open(path(path));

                return false;
            }
        }, GridException.class, msg);
    }

    /**
     * Validate directory listing.
     *
     * @param path Directory path to validate listing for.
     * @param item List of directory items.
     * @throws GridException If failed.
     */
    private void assertListDir(String path, String... item) throws GridException {
        Collection<IgniteFsFile> files = ggfs.listFiles(new IgniteFsPath(path));

        List<String> names = new ArrayList<>(item.length);

        for (IgniteFsFile file : files)
            names.add(file.path().name());

        Arrays.sort(item);
        Collections.sort(names);

        assertEquals(Arrays.asList(item), names);
    }
}
