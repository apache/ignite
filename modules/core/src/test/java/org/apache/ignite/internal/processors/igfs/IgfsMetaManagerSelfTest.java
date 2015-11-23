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

        assertTrue(mgr.mkdirs(new IgfsPath("/dir"), IgfsImpl.DFLT_DIR_META));
        assertNotNull(mgr.create(new IgfsPath("/file"), false, false, null, 400, null, false, null));

        IgfsListingEntry dirEntry = mgr.directoryListing(ROOT_ID).get("dir");
        assertNotNull(dirEntry);
        assertTrue(dirEntry.isDirectory());
        IgfsFileInfo dir = mgr.info(dirEntry.fileId());

        IgfsListingEntry fileEntry = mgr.directoryListing(ROOT_ID).get("file");
        assertNotNull(fileEntry);
        assertTrue(!fileEntry.isDirectory());
        IgfsFileInfo file = mgr.info(fileEntry.fileId());

        assertEquals(2, mgr.directoryListing(ROOT_ID).size());

        for (IgniteBiTuple<IgniteUuid, String> tup: Arrays.asList(F.t(dir.id(), "dir"), F.t(file.id(), "file"))) {
            IgniteUuid fileId = tup.get1();
            String fileName = tup.get2();

            for (Map<String, String> props : Arrays.asList(null, Collections.<String, String>emptyMap()))
                expectsUpdatePropertiesFail(fileId, props, AssertionError.class, "Expects not-empty file's properties");

            String key1 = UUID.randomUUID().toString();
            String key2 = UUID.randomUUID().toString();

            IgfsFileInfo info = mgr.info(fileId);

            assertNull("Unexpected stored properties: " + info, info.properties().get(key1));
            assertNull("Unexpected stored properties: " + info, info.properties().get(key2));

            info = mgr.updateProperties(ROOT_ID, fileId, fileName, F.asMap(key1, "1"));

            assertEquals("Unexpected stored properties: " + info, "1", info.properties().get(key1));

            info = mgr.updateProperties(ROOT_ID, fileId, fileName, F.asMap(key2, "2"));

           // assertEquals("Unexpected stored properties: " + info, F.asMap(key1, "1", key2, "2"), info.properties());
            assertEquals("Unexpected stored properties: " + info, "1", info.properties().get(key1));
            assertEquals("Unexpected stored properties: " + info, "2", info.properties().get(key2));

            info = mgr.updateProperties(ROOT_ID, fileId, fileName, F.<String, String>asMap(key1, null));

            assertEquals("Unexpected stored properties: " + info, "2", info.properties().get(key2));

            info = mgr.updateProperties(ROOT_ID, fileId, fileName, F.<String, String>asMap(key2, null));

            assertNull("Unexpected stored properties: " + info, info.properties().get(key1));
            assertNull("Unexpected stored properties: " + info, info.properties().get(key2));

            assertNull(mgr.updateProperties(ROOT_ID, fileId, "not_exists", F.<String, String>asMap(key2, null)));
        }

        mgr.softDelete(new IgfsPath("/dir"), true);
        mgr.softDelete(new IgfsPath("/file"), false);

        assertNull(mgr.updateProperties(ROOT_ID, dir.id(), "dir", F.asMap("p", "7")));
        assertNull(mgr.updateProperties(ROOT_ID, file.id(), "file", F.asMap("q", "8")));
    }

    private IgfsFileInfo mkdirsAndGetInfo(String path) throws IgniteCheckedException {
        IgfsPath p = path(path);

        mgr.mkdirs(p, IgfsImpl.DFLT_DIR_META);

        IgniteUuid id = mgr.fileId(p);

        IgfsFileInfo info = mgr.info(id);

        assert info.isDirectory();

        return info;
    }

    private IgfsFileInfo createFileAndGetInfo(String path) throws IgniteCheckedException {
        IgfsPath p = path(path);

        IgniteBiTuple<IgfsFileInfo, IgniteUuid> t2 = mgr.create(p, false, false, null, 400, null, false, null);

        assert t2 != null;
        assert !t2.get1().isDirectory();

        return t2.get1();
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

        // Directories:
        IgfsFileInfo a = mkdirsAndGetInfo("/a");
        IgfsFileInfo b = mkdirsAndGetInfo("/a/b");
        IgfsFileInfo k = mkdirsAndGetInfo("/a/b/k");
        IgfsFileInfo z = mkdirsAndGetInfo("/a/k");

        // Files:
        IgfsFileInfo f1 = createFileAndGetInfo("/f1");
        IgfsFileInfo f2 = createFileAndGetInfo("/a/f2");
        IgfsFileInfo f3 = createFileAndGetInfo("/a/b/f3");

        assertEquals(F.asMap("a", new IgfsListingEntry(a), "f1", new IgfsListingEntry(f1)),
            mgr.directoryListing(ROOT_ID));

        assertEquals(F.asMap("b", new IgfsListingEntry(b), "f2", new IgfsListingEntry(f2), "k", new IgfsListingEntry(z)),
            mgr.directoryListing(a.id()));

        assertEquals(F.asMap("f3", new IgfsListingEntry(f3),
            "k", new IgfsListingEntry(k)), mgr.directoryListing(b.id()));

        // Validate empty files listings.
        for (IgfsFileInfo info : Arrays.asList(f1, f2, f3))
            assertEmpty(mgr.directoryListing(info.id()));

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

        // One of participated files does not exist in cache.
        expectsRenameFail("/b8", "/b2", "Failed to perform move because some path component was not found.");

        expectsRenameFail("/a", "/b/b8", "Failed to perform move because some path component was not found.");

        expectsRenameFail("/a/f2", "/a/b/f3", "Failed to perform move because destination points to existing file");

        expectsRenameFail("/a/k", "/a/b/", "Failed to perform move because destination already " +
            "contains entry with the same name existing file");

        mgr.delete(a.id(), "k", k.id());
        mgr.delete(b.id(), "k", z.id());

        System.out.println("/: " + mgr.directoryListing(ROOT_ID));
        System.out.println("a: " + mgr.directoryListing(a.id()));
        System.out.println("b: " + mgr.directoryListing(b.id()));
        System.out.println("f3: " + mgr.directoryListing(f3.id()));

        mgr.move(path("/a"), path("/a2"));
        mgr.move(path("/a2/b"), path("/a2/b2"));

        assertNotNull(mgr.info(b.id()));

        mgr.move(path("/a2/b2/f3"), path("/a2/b2/f3-2"));

        assertNotNull(mgr.info(b.id()));

        mgr.move(path("/a2/b2/f3-2"), path("/a2/b2/f3"));

        mgr.move(path("/a2/b2"), path("/a2/b"));

        mgr.move(path("/a2"), path("/a"));

        IgniteUuid del = mgr.softDelete(path("/a/b/f3"), false);

        assertEquals(f3.id(), del);

        assertEquals(F.asMap("a", new IgfsListingEntry(a), "f1", new IgfsListingEntry(f1)),
                mgr.directoryListing(ROOT_ID));

        assertEquals(
            F.asMap("b", new IgfsListingEntry(b),
                "f2", new IgfsListingEntry(f2)),
            mgr.directoryListing(a.id()));

        assertEmpty(mgr.directoryListing(b.id()));

        //assertEquals(b, mgr.removeIfEmpty(a.id(), "b", b.id(), new IgfsPath("/a/b"), true));
        del = mgr.softDelete(path("/a/b"), false);
        assertEquals(b.id(), del);

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

        del = mgr.softDelete(path("/a/f2"), false);
        assertEquals(f2.id(), del);

        assertEquals(F.asMap("a", new IgfsListingEntry(a), "f1", new IgfsListingEntry(f1)),
            mgr.directoryListing(ROOT_ID));

        assertEmpty(mgr.directoryListing(a.id()));
        assertEmpty(mgr.directoryListing(b.id()));

        del = mgr.softDelete(path("/f1"), false);
        assertEquals(f1.id(), del);

        assertEquals(F.asMap("a", new IgfsListingEntry(a)), mgr.directoryListing(ROOT_ID));

        assertEmpty(mgr.directoryListing(a.id()));
        assertEmpty(mgr.directoryListing(b.id()));

        del = mgr.softDelete(path("/a"), false);
        assertEquals(a.id(), del);

        assertEmpty(mgr.directoryListing(ROOT_ID));
        assertEmpty(mgr.directoryListing(a.id()));
        assertEmpty(mgr.directoryListing(b.id()));
    }

    /**
     * Utility method to make IgfsPath.
     *
     * @param p The String path.
     * @return The IgfsPath object.
     */
    private static IgfsPath path(String p) {
        return new IgfsPath(p);
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
            @Override public Object call() throws Exception {
                return mgr.updateProperties(null, fileId, "file", props);
            }
        }, cls, msg);
    }

    /**
     * Test expected failures for 'move file' operation.
     *
     * @param msg Failure message if expected exception was not thrown.
     */
    private void expectsRenameFail(final String src, final String dst, @Nullable String msg) {
        Throwable err = assertThrowsInherited(log, new Callable() {
            @Override public Object call() throws Exception {
                mgr.move(new IgfsPath(src), new IgfsPath(dst));

                return null;
            }
        }, IgfsException.class, msg);

        assertTrue("Unexpected cause: " + err, err instanceof IgfsException);
    }
}