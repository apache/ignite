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

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.igfs.IgfsDirectoryNotEmptyException;
import org.apache.ignite.igfs.IgfsException;
import org.apache.ignite.igfs.IgfsGroupDataBlocksKeyMapper;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.processors.igfs.IgfsFileInfo.ROOT_ID;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsInherited;
import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;

/**
 * {@link IgfsMetaManager} test case.
 */
public class IgfsMetaManagerSelfTest extends IgfsCommonAbstractTest {
    /** Test IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Meta-information cache name. */
    private static final String META_CACHE_NAME = "replicated";

    /** Data cache name. */
    public static final String DATA_CACHE_NAME = "data";

    /** Test nodes count. */
    private static final int NODES_CNT = 4;

    /** Meta manager to test. */
    private IgfsMetaManager mgr;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        IgfsEx igfs = (IgfsEx)grid(0).fileSystem("igfs");

        mgr = igfs.context().meta();
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

        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);

        return cacheCfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        mgr.igfsCtx.igfs().format();
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

        IgfsFileInfo dir = new IgfsFileInfo(true, null);
        IgfsFileInfo file = new IgfsFileInfo(new IgfsFileInfo(400, null, false, null), 1);

        assertNull(mgr.putIfAbsent(ROOT_ID, "dir", dir));
        assertNull(mgr.putIfAbsent(ROOT_ID, "file", file));

        assertEquals(F.asMap("dir", new IgfsListingEntry(dir), "file", new IgfsListingEntry(file)),
            mgr.directoryListing(ROOT_ID));

        //IgfsFileInfo tmp = mgr.info(dir.id());

        assertEquals(dir, mgr.info(dir.id()));
        assertEquals(file, mgr.info(file.id()));

        for (IgniteBiTuple<IgniteUuid, String> tup: Arrays.asList(F.t(dir.id(), "dir"), F.t(file.id(), "file"))) {
            IgniteUuid fileId = tup.get1();
            String fileName = tup.get2();

            for (Map<String, String> props : Arrays.asList(null, Collections.<String, String>emptyMap()))
                expectsUpdatePropertiesFail(fileId, props, AssertionError.class, "Expects not-empty file's properties");

            String key1 = UUID.randomUUID().toString();
            String key2 = UUID.randomUUID().toString();

            IgfsFileInfo info = mgr.info(fileId);

            assertNull("Expects empty properties are not stored: " + info, getFieldValue(info, "props"));
            assertEquals("Expects empty properties are not stored: " + info, Collections.<String, String>emptyMap(),
                info.properties());

            info = mgr.updateProperties(ROOT_ID, fileId, fileName, F.asMap(key1, "1"));

            assertEquals("Unexpected stored properties: " + info, F.asMap(key1, "1"), info.properties());

            info = mgr.updateProperties(ROOT_ID, fileId, fileName, F.asMap(key2, "2"));

            assertEquals("Unexpected stored properties: " + info, F.asMap(key1, "1", key2, "2"), info.properties());

            info = mgr.updateProperties(ROOT_ID, fileId, fileName, F.<String, String>asMap(key1, null));

            assertEquals("Unexpected stored properties: " + info, F.asMap(key2, "2"), info.properties());

            info = mgr.updateProperties(ROOT_ID, fileId, fileName, F.<String, String>asMap(key2, null));

            assertNull("Expects empty properties are not stored: " + info, getFieldValue(info, "props"));
            assertEquals("Expects empty properties are not stored: " + info, Collections.<String, String>emptyMap(),
                info.properties());

            assertNull(mgr.updateProperties(ROOT_ID, fileId, "not_exists", F.<String, String>asMap(key2, null)));
        }

        mgr.removeIfEmpty(ROOT_ID, "dir", dir.id(), new IgfsPath("/dir"), true);
        mgr.removeIfEmpty(ROOT_ID, "file", file.id(), new IgfsPath("/file"), true);

        assertNull(mgr.updateProperties(ROOT_ID, dir.id(), "dir", F.asMap("p", "7")));
        assertNull(mgr.updateProperties(ROOT_ID, file.id(), "file", F.asMap("q", "8")));
    }

    /**
     * Test file system structure in meta-cache.
     *
     * @throws Exception If failed.
     */
    public void testStructure() throws Exception {
        IgfsFileInfo rootInfo = new IgfsFileInfo();
        // Test empty structure.
        assertEmpty(mgr.directoryListing(ROOT_ID));
        assertEquals(rootInfo, mgr.info(ROOT_ID));
        assertEquals(F.asMap(ROOT_ID, rootInfo), mgr.infos(Arrays.asList(ROOT_ID)));

        IgfsFileInfo a = new IgfsFileInfo(true, null);
        IgfsFileInfo b = new IgfsFileInfo(true, null);
        IgfsFileInfo f1 = new IgfsFileInfo(400, null, false, null);
        IgfsFileInfo f2 = new IgfsFileInfo(new IgfsFileInfo(400, null, false, null), 0);
        IgfsFileInfo f3 = new IgfsFileInfo(new IgfsFileInfo(400, null, false, null), 200000L);

        // Validate 'add file' operation.
        assertNull(mgr.putIfAbsent(ROOT_ID, "a", a));
        assertNull(mgr.putIfAbsent(ROOT_ID, "f1", f1));
        assertNull(mgr.putIfAbsent(a.id(), "b", b));
        assertNull(mgr.putIfAbsent(a.id(), "f2", f2));
        assertNull(mgr.putIfAbsent(b.id(), "f3", f3));

        assertEquals(b.id(), mgr.putIfAbsent(a.id(), "b", f3));
        expectsPutIfAbsentFail(a.id(), "c", f3, "Failed to add file details into cache");

        assertEquals(F.asMap("a", new IgfsListingEntry(a), "f1", new IgfsListingEntry(f1)),
            mgr.directoryListing(ROOT_ID));

        assertEquals(F.asMap("b", new IgfsListingEntry(b), "f2", new IgfsListingEntry(f2)),
            mgr.directoryListing(a.id()));

        assertEquals(F.asMap("f3", new IgfsListingEntry(f3)), mgr.directoryListing(b.id()));

        // Validate empty files listings.
        for (IgfsFileInfo info : Arrays.asList(f1, f2, f3)) {
            assertEmpty(mgr.directoryListing(info.id()));
        }

        // Validate 'file info' operations.
        for (IgfsFileInfo info : Arrays.asList(rootInfo, a, b, f1, f2, f3)) {
            assertEquals(info, mgr.info(info.id()));
            assertEquals(F.asMap(info.id(), info), mgr.infos(Arrays.asList(info.id())));
        }

        // Validate 'file ID' operations.
        assertEquals(ROOT_ID, mgr.fileId(new IgfsPath("/")));
        assertEquals(a.id(), mgr.fileId(new IgfsPath("/a")));
        assertEquals(b.id(), mgr.fileId(new IgfsPath("/a/b")));
        assertEquals(f1.id(), mgr.fileId(new IgfsPath("/f1")));
        assertEquals(f2.id(), mgr.fileId(new IgfsPath("/a/f2")));
        assertEquals(f3.id(), mgr.fileId(new IgfsPath("/a/b/f3")));
        assertNull(mgr.fileId(new IgfsPath("/f4")));
        assertNull(mgr.fileId(new IgfsPath("/a/f5")));
        assertNull(mgr.fileId(new IgfsPath("/a/b/f6")));

        assertEquals(a.id(), mgr.fileId(ROOT_ID, "a"));
        assertEquals(b.id(), mgr.fileId(a.id(), "b"));
        assertEquals(f1.id(), mgr.fileId(ROOT_ID, "f1"));
        assertEquals(f2.id(), mgr.fileId(a.id(), "f2"));
        assertEquals(f3.id(), mgr.fileId(b.id(), "f3"));
        assertNull(mgr.fileId(ROOT_ID, "f4"));
        assertNull(mgr.fileId(a.id(), "f5"));
        assertNull(mgr.fileId(b.id(), "f6"));

        assertEquals(Arrays.asList(ROOT_ID), mgr.fileIds(new IgfsPath("/")));
        assertEquals(Arrays.asList(ROOT_ID, a.id()), mgr.fileIds(new IgfsPath("/a")));
        assertEquals(Arrays.asList(ROOT_ID, a.id(), b.id()), mgr.fileIds(new IgfsPath("/a/b")));
        assertEquals(Arrays.asList(ROOT_ID, f1.id()), mgr.fileIds(new IgfsPath("/f1")));
        assertEquals(Arrays.asList(ROOT_ID, a.id(), f2.id()), mgr.fileIds(new IgfsPath("/a/f2")));
        assertEquals(Arrays.asList(ROOT_ID, a.id(), b.id(), f3.id()), mgr.fileIds(new IgfsPath("/a/b/f3")));
        assertEquals(Arrays.asList(ROOT_ID, null), mgr.fileIds(new IgfsPath("/f4")));
        assertEquals(Arrays.asList(ROOT_ID, a.id(), null), mgr.fileIds(new IgfsPath("/a/f5")));
        assertEquals(Arrays.asList(ROOT_ID, a.id(), b.id(), null), mgr.fileIds(new IgfsPath("/a/b/f6")));
        assertEquals(Arrays.asList(ROOT_ID, null, null, null, null), mgr.fileIds(new IgfsPath("/f7/a/b/f6")));

        // Validate 'rename' operation.
        final IgniteUuid rndId = IgniteUuid.randomUuid();

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
            assertNull(mgr.removeIfEmpty(ROOT_ID, "a", IgniteUuid.randomUuid(), new IgfsPath("/a"), true));
            assertNull(mgr.removeIfEmpty(IgniteUuid.randomUuid(), "a", IgniteUuid.randomUuid(),
                new IgfsPath("/" + IgniteUuid.randomUuid() + "/a"), true));
        }

        expectsRemoveFail(ROOT_ID, "a", a.id(), new IgfsPath("/a"),
            "Failed to remove file (directory is not empty)");
        expectsRemoveFail(a.id(), "b", b.id(), new IgfsPath("/a/b"),
            "Failed to remove file (directory is not empty)");
        assertNull(mgr.removeIfEmpty(ROOT_ID, "a", f1.id(), new IgfsPath("/a"), true));
        assertNull(mgr.removeIfEmpty(a.id(), "b", f1.id(), new IgfsPath("/a/b"), true));

        assertEquals(f3, mgr.removeIfEmpty(b.id(), "f3", f3.id(), new IgfsPath("/a/b/f3"), true));

        assertEquals(F.asMap("a", new IgfsListingEntry(a), "f1", new IgfsListingEntry(f1)),
            mgr.directoryListing(ROOT_ID));

        assertEquals(F.asMap("b", new IgfsListingEntry(b), "f2", new IgfsListingEntry(f2)),
            mgr.directoryListing(a.id()));

        assertEmpty(mgr.directoryListing(b.id()));

        assertEquals(b, mgr.removeIfEmpty(a.id(), "b", b.id(), new IgfsPath("/a/b"), true));

        assertEquals(F.asMap("a", new IgfsListingEntry(a), "f1", new IgfsListingEntry(f1)),
            mgr.directoryListing(ROOT_ID));

        assertEquals(F.asMap("f2", new IgfsListingEntry(f2)), mgr.directoryListing(a.id()));

        assertEmpty(mgr.directoryListing(b.id()));

        // Validate last actual data received from 'remove' operation.
        IgfsFileInfo newF2 = mgr.updateInfo(f2.id(), new C1<IgfsFileInfo, IgfsFileInfo>() {
            @Override public IgfsFileInfo apply(IgfsFileInfo e) {
                return new IgfsFileInfo(e, e.length() + 20);
            }
        });

        assertNotNull(newF2);
        assertEquals(f2.id(), newF2.id());
        assertNotSame(f2, newF2);

        assertEquals(newF2, mgr.removeIfEmpty(a.id(), "f2", f2.id(), new IgfsPath("/a/f2"), true));

        assertEquals(F.asMap("a", new IgfsListingEntry(a), "f1", new IgfsListingEntry(f1)),
            mgr.directoryListing(ROOT_ID));

        assertEmpty(mgr.directoryListing(a.id()));
        assertEmpty(mgr.directoryListing(b.id()));

        assertEquals(f1, mgr.removeIfEmpty(ROOT_ID, "f1", f1.id(), new IgfsPath("/f1"), true));

        assertEquals(F.asMap("a", new IgfsListingEntry(a)), mgr.directoryListing(ROOT_ID));

        assertEmpty(mgr.directoryListing(a.id()));
        assertEmpty(mgr.directoryListing(b.id()));

        assertEquals(a, mgr.removeIfEmpty(ROOT_ID, "a", a.id(), new IgfsPath("/a"), true));

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
    private void expectsUpdatePropertiesFail(@Nullable final IgniteUuid fileId, @Nullable final Map<String, String> props,
        Class<? extends Throwable> cls, @Nullable String msg) {
        assertThrows(log, new Callable() {
            @Override
            public Object call() throws Exception {
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
    private void expectsPutIfAbsentFail(final IgniteUuid parentId, final String fileName, final IgfsFileInfo fileInfo,
        @Nullable String msg) {
        Throwable err = assertThrows(log, new Callable() {
            @Override public Object call() throws Exception {
                return mgr.putIfAbsent(parentId, fileName, fileInfo);
            }
        }, IgniteCheckedException.class, msg);

        assertTrue("Unexpected cause: " + err.getCause(), err.getCause() instanceof IgfsException);
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
    private void expectsRenameFail(final IgniteUuid fileId, final String srcFileName, final IgniteUuid srcParentId,
        final String destFileName, final IgniteUuid destParentId, @Nullable String msg) {
        Throwable err = assertThrowsInherited(log, new Callable() {
            @Override
            public Object call() throws Exception {
                mgr.move(fileId, srcFileName, srcParentId, destFileName, destParentId);

                return null;
            }
        }, IgniteCheckedException.class, msg);

        assertTrue("Unexpected cause: " + err.getCause(), err.getCause() instanceof IgfsException);
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
    private void expectsRemoveFail(final IgniteUuid parentId, final String fileName, final IgniteUuid fileId,
        final IgfsPath path, @Nullable String msg) {
        Throwable err = assertThrows(log, new Callable() {
            @Nullable @Override public Object call() throws Exception {
                mgr.removeIfEmpty(parentId, fileName, fileId, path, true);

                return null;
            }
        }, IgniteCheckedException.class, msg);

        assertTrue("Unexpected cause: " + err.getCause(), err.getCause() instanceof IgfsDirectoryNotEmptyException);
    }
}