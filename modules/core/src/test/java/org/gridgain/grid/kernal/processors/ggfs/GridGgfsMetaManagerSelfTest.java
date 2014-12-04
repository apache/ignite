/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.ggfs;

import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;
import static org.gridgain.grid.kernal.processors.ggfs.GridGgfsFileInfo.*;
import static org.gridgain.testframework.GridTestUtils.*;

/**
 * {@link GridGgfsMetaManager} test case.
 */
public class GridGgfsMetaManagerSelfTest extends GridGgfsCommonAbstractTest {
    /** Test IP finder. */
    private static final GridTcpDiscoveryIpFinder IP_FINDER = new GridTcpDiscoveryVmIpFinder(true);

    /** Meta-information cache name. */
    private static final String META_CACHE_NAME = "replicated";

    /** Data cache name. */
    public static final String DATA_CACHE_NAME = "data";

    /** Test nodes count. */
    private static final int NODES_CNT = 4;

    /** Meta manager to test. */
    private GridGgfsMetaManager mgr;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        GridGgfsEx ggfs = (GridGgfsEx)grid(0).ggfs("ggfs");

        mgr = ggfs.context().meta();
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
            cacheCfg.setDistributionMode(PARTITIONED_ONLY);

            cacheCfg.setBackups(0);
            cacheCfg.setAffinityMapper(new GridGgfsGroupDataBlocksKeyMapper(128));
        }

        cacheCfg.setQueryIndexEnabled(false);
        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);

        return cacheCfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        mgr.ggfsCtx.ggfs().format();
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
     * Test properties management in meta-cache.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("NullableProblems")
    public void testUpdateProperties() throws Exception {
        assertEmpty(mgr.directoryListing(ROOT_ID));

        GridGgfsFileInfo dir = new GridGgfsFileInfo(true, null);
        GridGgfsFileInfo file = new GridGgfsFileInfo(new GridGgfsFileInfo(400, null, false, null), 1);

        assertNull(mgr.putIfAbsent(ROOT_ID, "dir", dir));
        assertNull(mgr.putIfAbsent(ROOT_ID, "file", file));

        assertEquals(F.asMap("dir", new GridGgfsListingEntry(dir), "file", new GridGgfsListingEntry(file)),
            mgr.directoryListing(ROOT_ID));

        //GridGgfsFileInfo tmp = mgr.info(dir.id());

        assertEquals(dir, mgr.info(dir.id()));
        assertEquals(file, mgr.info(file.id()));

        for (IgniteBiTuple<GridUuid, String> tup: Arrays.asList(F.t(dir.id(), "dir"), F.t(file.id(), "file"))) {
            GridUuid fileId = tup.get1();
            String fileName = tup.get2();

            for (Map<String, String> props : Arrays.asList(null, Collections.<String, String>emptyMap()))
                expectsUpdatePropertiesFail(fileId, props, AssertionError.class, "Expects not-empty file's properties");

            String key1 = UUID.randomUUID().toString();
            String key2 = UUID.randomUUID().toString();

            GridGgfsFileInfo info = mgr.info(fileId);

            assertNull("Expects empty properties are not stored: " + info, getFieldValue(info, "props"));
            assertEquals("Expects empty properties are not stored: " + info, Collections.emptyMap(), info.properties());

            info = mgr.updateProperties(ROOT_ID, fileId, fileName, F.asMap(key1, "1"));

            assertEquals("Unexpected stored properties: " + info, F.asMap(key1, "1"), info.properties());

            info = mgr.updateProperties(ROOT_ID, fileId, fileName, F.asMap(key2, "2"));

            assertEquals("Unexpected stored properties: " + info, F.asMap(key1, "1", key2, "2"), info.properties());

            info = mgr.updateProperties(ROOT_ID, fileId, fileName, F.<String, String>asMap(key1, null));

            assertEquals("Unexpected stored properties: " + info, F.asMap(key2, "2"), info.properties());

            info = mgr.updateProperties(ROOT_ID, fileId, fileName, F.<String, String>asMap(key2, null));

            assertNull("Expects empty properties are not stored: " + info, getFieldValue(info, "props"));
            assertEquals("Expects empty properties are not stored: " + info, Collections.emptyMap(), info.properties());

            assertNull(mgr.updateProperties(ROOT_ID, fileId, "not_exists", F.<String, String>asMap(key2, null)));
        }

        mgr.removeIfEmpty(ROOT_ID, "dir", dir.id(), new GridGgfsPath("/dir"), true);
        mgr.removeIfEmpty(ROOT_ID, "file", file.id(), new GridGgfsPath("/file"), true);

        assertNull(mgr.updateProperties(ROOT_ID, dir.id(), "dir", F.asMap("p", "7")));
        assertNull(mgr.updateProperties(ROOT_ID, file.id(), "file", F.asMap("q", "8")));
    }

    /**
     * Test file system structure in meta-cache.
     *
     * @throws Exception If failed.
     */
    public void testStructure() throws Exception {
        GridGgfsFileInfo rootInfo = new GridGgfsFileInfo();
        // Test empty structure.
        assertEmpty(mgr.directoryListing(ROOT_ID));
        assertEquals(rootInfo, mgr.info(ROOT_ID));
        assertEquals(F.asMap(ROOT_ID, rootInfo), mgr.infos(Arrays.asList(ROOT_ID)));

        GridGgfsFileInfo a = new GridGgfsFileInfo(true, null);
        GridGgfsFileInfo b = new GridGgfsFileInfo(true, null);
        GridGgfsFileInfo f1 = new GridGgfsFileInfo(400, null, false, null);
        GridGgfsFileInfo f2 = new GridGgfsFileInfo(new GridGgfsFileInfo(400, null, false, null), 0);
        GridGgfsFileInfo f3 = new GridGgfsFileInfo(new GridGgfsFileInfo(400, null, false, null), 200000L);

        // Validate 'add file' operation.
        assertNull(mgr.putIfAbsent(ROOT_ID, "a", a));
        assertNull(mgr.putIfAbsent(ROOT_ID, "f1", f1));
        assertNull(mgr.putIfAbsent(a.id(), "b", b));
        assertNull(mgr.putIfAbsent(a.id(), "f2", f2));
        assertNull(mgr.putIfAbsent(b.id(), "f3", f3));

        assertEquals(b.id(), mgr.putIfAbsent(a.id(), "b", f3));
        expectsPutIfAbsentFail(a.id(), "c", f3, "Failed to add file details into cache");

        assertEquals(F.asMap("a", new GridGgfsListingEntry(a), "f1", new GridGgfsListingEntry(f1)),
            mgr.directoryListing(ROOT_ID));

        assertEquals(F.asMap("b", new GridGgfsListingEntry(b), "f2", new GridGgfsListingEntry(f2)),
            mgr.directoryListing(a.id()));

        assertEquals(F.asMap("f3", new GridGgfsListingEntry(f3)), mgr.directoryListing(b.id()));

        // Validate empty files listings.
        for (GridGgfsFileInfo info : Arrays.asList(f1, f2, f3)) {
            assertEmpty(mgr.directoryListing(info.id()));
        }

        // Validate 'file info' operations.
        for (GridGgfsFileInfo info : Arrays.asList(rootInfo, a, b, f1, f2, f3)) {
            assertEquals(info, mgr.info(info.id()));
            assertEquals(F.asMap(info.id(), info), mgr.infos(Arrays.asList(info.id())));
        }

        // Validate 'file ID' operations.
        assertEquals(ROOT_ID, mgr.fileId(new GridGgfsPath("/")));
        assertEquals(a.id(), mgr.fileId(new GridGgfsPath("/a")));
        assertEquals(b.id(), mgr.fileId(new GridGgfsPath("/a/b")));
        assertEquals(f1.id(), mgr.fileId(new GridGgfsPath("/f1")));
        assertEquals(f2.id(), mgr.fileId(new GridGgfsPath("/a/f2")));
        assertEquals(f3.id(), mgr.fileId(new GridGgfsPath("/a/b/f3")));
        assertNull(mgr.fileId(new GridGgfsPath("/f4")));
        assertNull(mgr.fileId(new GridGgfsPath("/a/f5")));
        assertNull(mgr.fileId(new GridGgfsPath("/a/b/f6")));

        assertEquals(a.id(), mgr.fileId(ROOT_ID, "a"));
        assertEquals(b.id(), mgr.fileId(a.id(), "b"));
        assertEquals(f1.id(), mgr.fileId(ROOT_ID, "f1"));
        assertEquals(f2.id(), mgr.fileId(a.id(), "f2"));
        assertEquals(f3.id(), mgr.fileId(b.id(), "f3"));
        assertNull(mgr.fileId(ROOT_ID, "f4"));
        assertNull(mgr.fileId(a.id(), "f5"));
        assertNull(mgr.fileId(b.id(), "f6"));

        assertEquals(Arrays.asList(ROOT_ID), mgr.fileIds(new GridGgfsPath("/")));
        assertEquals(Arrays.asList(ROOT_ID, a.id()), mgr.fileIds(new GridGgfsPath("/a")));
        assertEquals(Arrays.asList(ROOT_ID, a.id(), b.id()), mgr.fileIds(new GridGgfsPath("/a/b")));
        assertEquals(Arrays.asList(ROOT_ID, f1.id()), mgr.fileIds(new GridGgfsPath("/f1")));
        assertEquals(Arrays.asList(ROOT_ID, a.id(), f2.id()), mgr.fileIds(new GridGgfsPath("/a/f2")));
        assertEquals(Arrays.asList(ROOT_ID, a.id(), b.id(), f3.id()), mgr.fileIds(new GridGgfsPath("/a/b/f3")));
        assertEquals(Arrays.asList(ROOT_ID, null), mgr.fileIds(new GridGgfsPath("/f4")));
        assertEquals(Arrays.asList(ROOT_ID, a.id(), null), mgr.fileIds(new GridGgfsPath("/a/f5")));
        assertEquals(Arrays.asList(ROOT_ID, a.id(), b.id(), null), mgr.fileIds(new GridGgfsPath("/a/b/f6")));
        assertEquals(Arrays.asList(ROOT_ID, null, null, null, null), mgr.fileIds(new GridGgfsPath("/f7/a/b/f6")));

        // Validate 'rename' operation.
        final GridUuid rndId = GridUuid.randomUuid();

        // One of participated files does not exist in cache.
        expectsRenameFail(ROOT_ID, "b", rndId, "b2", rndId, "Failed to lock source directory (not found?)");
        expectsRenameFail(b.id(), "b", rndId, "b2", rndId, "Failed to lock source directory (not found?)");
        expectsRenameFail(ROOT_ID, "b", ROOT_ID, "b2", rndId, "Failed to lock destination directory (not found?)");
        expectsRenameFail(b.id(), "b", ROOT_ID, "b2", rndId, "Failed to lock destination directory (not found?)");
        expectsRenameFail(rndId, "b", ROOT_ID, "b2", ROOT_ID, "Failed to lock target file (not found?)");
        expectsRenameFail(rndId, "b", b.id(), "b2", b.id(), "Failed to lock target file (not found?)");

        // Target file ID differ from the file ID resolved from the source directory for source file name.
        expectsRenameFail(b.id(), "a", ROOT_ID, "q", ROOT_ID, "Failed to remove file name from the source directory");
        expectsRenameFail(f1.id(), "a", ROOT_ID, "q", ROOT_ID, "Failed to remove file name from the source directory");
        expectsRenameFail(f2.id(), "a", ROOT_ID, "q", ROOT_ID, "Failed to remove file name from the source directory");
        expectsRenameFail(f3.id(), "a", ROOT_ID, "q", ROOT_ID, "Failed to remove file name from the source directory");

        // Invalid source file name (not found).
        expectsRenameFail(a.id(), "u1", ROOT_ID, "q", ROOT_ID, "Failed to remove file name from the source");
        expectsRenameFail(a.id(), "u2", ROOT_ID, "q", ROOT_ID, "Failed to remove file name from the source");
        expectsRenameFail(a.id(), "u3", ROOT_ID, "q", ROOT_ID, "Failed to remove file name from the source");

        // Invalid destination file - already exists.
        expectsRenameFail(a.id(), "a", ROOT_ID, "f1", ROOT_ID, "Failed to add file name into the destination");
        expectsRenameFail(f2.id(), "f2", a.id(), "f1", ROOT_ID, "Failed to add file name into the destination");
        expectsRenameFail(f3.id(), "f3", b.id(), "f1", ROOT_ID, "Failed to add file name into the destination");
        expectsRenameFail(b.id(), "b", a.id(), "f2", a.id(), "Failed to add file name into the destination");

        System.out.println("/: " + mgr.directoryListing(ROOT_ID));
        System.out.println("a: " + mgr.directoryListing(a.id()));
        System.out.println("b: " + mgr.directoryListing(b.id()));
        System.out.println("f3: " + mgr.directoryListing(f3.id()));

        mgr.move(a.id(), "a", ROOT_ID, "a2", ROOT_ID);
        mgr.move(b.id(), "b", a.id(), "b2", a.id());

        assertNotNull(mgr.info(b.id()));

        mgr.move(f3.id(), "f3", b.id(), "f3-2", a.id());

        assertNotNull(mgr.info(b.id()));

        mgr.move(f3.id(), "f3-2", a.id(), "f3", b.id());
        mgr.move(b.id(), "b2", a.id(), "b", a.id());
        mgr.move(a.id(), "a2", ROOT_ID, "a", ROOT_ID);

        // Validate 'remove' operation.
        for (int i = 0; i < 100; i++) {
            // One of participants doesn't exist.
            assertNull(mgr.removeIfEmpty(ROOT_ID, "a", GridUuid.randomUuid(), new GridGgfsPath("/a"), true));
            assertNull(mgr.removeIfEmpty(GridUuid.randomUuid(), "a", GridUuid.randomUuid(),
                new GridGgfsPath("/" + GridUuid.randomUuid() + "/a"), true));
        }

        expectsRemoveFail(ROOT_ID, "a", a.id(), new GridGgfsPath("/a"),
            "Failed to remove file (directory is not empty)");
        expectsRemoveFail(a.id(), "b", b.id(), new GridGgfsPath("/a/b"),
            "Failed to remove file (directory is not empty)");
        assertNull(mgr.removeIfEmpty(ROOT_ID, "a", f1.id(), new GridGgfsPath("/a"), true));
        assertNull(mgr.removeIfEmpty(a.id(), "b", f1.id(), new GridGgfsPath("/a/b"), true));

        assertEquals(f3, mgr.removeIfEmpty(b.id(), "f3", f3.id(), new GridGgfsPath("/a/b/f3"), true));

        assertEquals(F.asMap("a", new GridGgfsListingEntry(a), "f1", new GridGgfsListingEntry(f1)),
            mgr.directoryListing(ROOT_ID));

        assertEquals(F.asMap("b", new GridGgfsListingEntry(b), "f2", new GridGgfsListingEntry(f2)),
            mgr.directoryListing(a.id()));

        assertEmpty(mgr.directoryListing(b.id()));

        assertEquals(b, mgr.removeIfEmpty(a.id(), "b", b.id(), new GridGgfsPath("/a/b"), true));

        assertEquals(F.asMap("a", new GridGgfsListingEntry(a), "f1", new GridGgfsListingEntry(f1)),
            mgr.directoryListing(ROOT_ID));

        assertEquals(F.asMap("f2", new GridGgfsListingEntry(f2)), mgr.directoryListing(a.id()));

        assertEmpty(mgr.directoryListing(b.id()));

        // Validate last actual data received from 'remove' operation.
        GridGgfsFileInfo newF2 = mgr.updateInfo(f2.id(), new C1<GridGgfsFileInfo, GridGgfsFileInfo>() {
            @Override public GridGgfsFileInfo apply(GridGgfsFileInfo e) {
                return new GridGgfsFileInfo(e, e.length() + 20);
            }
        });

        assertNotNull(newF2);
        assertEquals(f2.id(), newF2.id());
        assertNotSame(f2, newF2);

        assertEquals(newF2, mgr.removeIfEmpty(a.id(), "f2", f2.id(), new GridGgfsPath("/a/f2"), true));

        assertEquals(F.asMap("a", new GridGgfsListingEntry(a), "f1", new GridGgfsListingEntry(f1)),
            mgr.directoryListing(ROOT_ID));

        assertEmpty(mgr.directoryListing(a.id()));
        assertEmpty(mgr.directoryListing(b.id()));

        assertEquals(f1, mgr.removeIfEmpty(ROOT_ID, "f1", f1.id(), new GridGgfsPath("/f1"), true));

        assertEquals(F.asMap("a", new GridGgfsListingEntry(a)), mgr.directoryListing(ROOT_ID));

        assertEmpty(mgr.directoryListing(a.id()));
        assertEmpty(mgr.directoryListing(b.id()));

        assertEquals(a, mgr.removeIfEmpty(ROOT_ID, "a", a.id(), new GridGgfsPath("/a"), true));

        assertEmpty(mgr.directoryListing(ROOT_ID));
        assertEmpty(mgr.directoryListing(a.id()));
        assertEmpty(mgr.directoryListing(b.id()));
    }

    /**
     * Validate passed map is empty.
     *
     * @param map Map to validate it is empty.
     */
    private void assertEmpty(Map map) {
        assertEquals(Collections.emptyMap(), map);
    }

    /**
     * Test expected failures for 'update properties' operation.
     *
     * @param fileId File ID.
     * @param props File properties to set.
     * @param msg Failure message if expected exception was not thrown.
     */
    private void expectsUpdatePropertiesFail(@Nullable final GridUuid fileId, @Nullable final Map<String, String> props,
        Class<? extends Throwable> cls, @Nullable String msg) {
        GridTestUtils.assertThrows(log, new Callable() {
            @Override public Object call() throws Exception {
                return mgr.updateProperties(null, fileId, "file", props);
            }
        }, cls, msg);
    }

    /**
     * Test expected failures for 'add file' operation.
     *
     * @param parentId Parent file ID.
     * @param fileName New file name in the parent's listing.
     * @param fileInfo New file initial details.
     * @param msg Failure message if expected exception was not thrown.
     */
    private void expectsPutIfAbsentFail(final GridUuid parentId, final String fileName, final GridGgfsFileInfo fileInfo,
        @Nullable String msg) {
        GridTestUtils.assertThrows(log, new Callable() {
            @Override public Object call() throws Exception {
                return mgr.putIfAbsent(parentId, fileName, fileInfo);
            }
        }, GridGgfsException.class, msg);
    }

    /**
     * Test expected failures for 'move file' operation.
     *
     * @param fileId File ID to rename.
     * @param srcFileName Original file name in the parent's listing.
     * @param srcParentId Source parent directory ID.
     * @param destFileName New file name in the parent's listing after renaming.
     * @param destParentId Destination parent directory ID.
     * @param msg Failure message if expected exception was not thrown.
     */
    private void expectsRenameFail(final GridUuid fileId, final String srcFileName, final GridUuid srcParentId,
        final String destFileName, final GridUuid destParentId, @Nullable String msg) {
        GridTestUtils.assertThrowsInherited(log, new Callable() {
            @Override public Object call() throws Exception {
                mgr.move(fileId, srcFileName, srcParentId, destFileName, destParentId);

                return null;
            }
        }, GridGgfsException.class, msg);
    }

    /**
     * Test expected failures for 'remove file' operation.
     *
     * @param parentId Parent file ID to remove file from.
     * @param fileName File name in the parent's listing.
     * @param fileId File ID to remove.
     * @param path Removed file path.
     * @param msg Failure message if expected exception was not thrown.
     */
    private void expectsRemoveFail(final GridUuid parentId, final String fileName, final GridUuid fileId,
        final GridGgfsPath path, @Nullable String msg) {
        assertThrows(log, new Callable() {
            @Nullable @Override public Object call() throws Exception {
                mgr.removeIfEmpty(parentId, fileName, fileId, path, true);

                return null;
            }
        }, GridGgfsDirectoryNotEmptyException.class, msg);
    }
}
