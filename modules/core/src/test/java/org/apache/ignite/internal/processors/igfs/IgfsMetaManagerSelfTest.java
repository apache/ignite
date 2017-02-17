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
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.igfs.IgfsException;
import org.apache.ignite.igfs.IgfsGroupDataBlocksKeyMapper;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.processors.igfs.IgfsUtils.ROOT_ID;
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
        assertNotNull(mgr.create(new IgfsPath("/file"), null, false, 400, null, false, null, null));

        IgfsListingEntry dirEntry = mgr.directoryListing(ROOT_ID).get("dir");
        assertNotNull(dirEntry);
        assertTrue(dirEntry.isDirectory());
        IgfsEntryInfo dir = mgr.info(dirEntry.fileId());

        IgfsListingEntry fileEntry = mgr.directoryListing(ROOT_ID).get("file");
        assertNotNull(fileEntry);
        assertTrue(!fileEntry.isDirectory());
        IgfsEntryInfo file = mgr.info(fileEntry.fileId());

        assertEquals(2, mgr.directoryListing(ROOT_ID).size());

        for (IgniteBiTuple<IgniteUuid, String> tup: Arrays.asList(F.t(dir.id(), "dir"), F.t(file.id(), "file"))) {
            IgniteUuid fileId = tup.get1();

            for (Map<String, String> props : Arrays.asList(null, Collections.<String, String>emptyMap()))
                expectsUpdatePropertiesFail(fileId, props, AssertionError.class, "Expects not-empty file's properties");

            String key1 = UUID.randomUUID().toString();
            String key2 = UUID.randomUUID().toString();

            IgfsEntryInfo info = mgr.info(fileId);

            assertNull("Unexpected stored properties: " + info, info.properties().get(key1));
            assertNull("Unexpected stored properties: " + info, info.properties().get(key2));

            info = mgr.updateProperties(fileId, F.asMap(key1, "1"));

            assertEquals("Unexpected stored properties: " + info, "1", info.properties().get(key1));

            info = mgr.updateProperties(fileId, F.asMap(key2, "2"));

           // assertEquals("Unexpected stored properties: " + info, F.asMap(key1, "1", key2, "2"), info.properties());
            assertEquals("Unexpected stored properties: " + info, "1", info.properties().get(key1));
            assertEquals("Unexpected stored properties: " + info, "2", info.properties().get(key2));

            info = mgr.updateProperties(fileId, F.<String, String>asMap(key1, null));

            assertEquals("Unexpected stored properties: " + info, "2", info.properties().get(key2));

            info = mgr.updateProperties(fileId, F.<String, String>asMap(key2, null));

            assertNull("Unexpected stored properties: " + info, info.properties().get(key1));
            assertNull("Unexpected stored properties: " + info, info.properties().get(key2));
        }

        mgr.softDelete(new IgfsPath("/dir"), true, null);
        mgr.softDelete(new IgfsPath("/file"), false, null);

        assertNull(mgr.updateProperties(dir.id(), F.asMap("p", "7")));
    }

    private IgfsEntryInfo mkdirsAndGetInfo(String path) throws IgniteCheckedException {
        IgfsPath p = path(path);

        mgr.mkdirs(p, IgfsImpl.DFLT_DIR_META);

        IgniteUuid id = mgr.fileId(p);

        IgfsEntryInfo info = mgr.info(id);

        assert info.isDirectory();

        return info;
    }

    private IgfsEntryInfo createFileAndGetInfo(String path) throws IgniteCheckedException {
        IgfsPath p = path(path);

        IgfsEntryInfo res = mgr.create(p, null, false, 400, null, false, null, null).info();

        assert res != null;
        assert !res.isDirectory();

        return res;
    }

    /**
     * Test file system structure in meta-cache.
     *
     * @throws Exception If failed.
     */
    public void testStructure() throws Exception {
        IgfsEntryInfo rootInfo = IgfsUtils.createDirectory(ROOT_ID);

        // Test empty structure.
        assertEmpty(mgr.directoryListing(ROOT_ID));
        assertEquals(rootInfo, mgr.info(ROOT_ID));
        assertEquals(F.asMap(ROOT_ID, rootInfo), mgr.infos(Arrays.asList(ROOT_ID)));

        // Directories:
        IgfsEntryInfo a = mkdirsAndGetInfo("/a");
        IgfsEntryInfo b = mkdirsAndGetInfo("/a/b");
        IgfsEntryInfo k = mkdirsAndGetInfo("/a/b/k");
        IgfsEntryInfo z = mkdirsAndGetInfo("/a/k");

        // Files:
        IgfsEntryInfo f1 = createFileAndGetInfo("/f1");
        IgfsEntryInfo f2 = createFileAndGetInfo("/a/f2");
        IgfsEntryInfo f3 = createFileAndGetInfo("/a/b/f3");

        assertEquals(F.asMap("a", new IgfsListingEntry(a), "f1", new IgfsListingEntry(f1)),
            mgr.directoryListing(ROOT_ID));

        assertEquals(F.asMap("b", new IgfsListingEntry(b), "f2", new IgfsListingEntry(f2), "k", new IgfsListingEntry(z)),
            mgr.directoryListing(a.id()));

        assertEquals(F.asMap("f3", new IgfsListingEntry(f3),
            "k", new IgfsListingEntry(k)), mgr.directoryListing(b.id()));

        // Validate empty files listings.
        for (IgfsEntryInfo info : Arrays.asList(f1, f2, f3))
            assertEmpty(mgr.directoryListing(info.id()));

        // Validate 'file info' operations.
        for (IgfsEntryInfo info : Arrays.asList(rootInfo, a, b, f1, f2, f3)) {
            assertEquals(info, mgr.info(info.id()));
            assertEquals(F.asMap(info.id(), info), mgr.infos(Arrays.asList(info.id())));
        }

        // Validate 'file ID' operations.
        assertEquals(ROOT_ID, mgr.fileId(IgfsPath.ROOT));
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

        assertEquals(Arrays.asList(ROOT_ID), mgr.fileIds(IgfsPath.ROOT));
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
        expectsRenameFail("/b8", "/b2");

        expectsRenameFail("/a", "/b/b8");

        expectsRenameFail("/a/f2", "/a/b/f3");

        expectsRenameFail("/a/k", "/a/b/");

        mgr.delete(a.id(), "k", z.id());
        mgr.delete(b.id(), "k", k.id());

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

        mgr.softDelete(path("/a/b/f3"), false, null);

        assertEquals(F.asMap("a", new IgfsListingEntry(a), "f1", new IgfsListingEntry(f1)),
                mgr.directoryListing(ROOT_ID));

        assertEquals(F.asMap("b", new IgfsListingEntry(b), "f2", new IgfsListingEntry(f2)),
            mgr.directoryListing(a.id()));

        assertEmpty(mgr.directoryListing(b.id()));

        //assertEquals(b, mgr.removeIfEmpty(a.id(), "b", b.id(), new IgfsPath("/a/b"), true));
        mgr.softDelete(path("/a/b"), false, null);

        assertEquals(F.asMap("a", new IgfsListingEntry(a), "f1", new IgfsListingEntry(f1)),
            mgr.directoryListing(ROOT_ID));

        assertEquals(F.asMap("f2", new IgfsListingEntry(f2)), mgr.directoryListing(a.id()));

        assertEmpty(mgr.directoryListing(b.id()));

        mgr.softDelete(path("/a/f2"), false, null);

        assertEquals(F.asMap("a", new IgfsListingEntry(a), "f1", new IgfsListingEntry(f1)),
            mgr.directoryListing(ROOT_ID));

        assertEmpty(mgr.directoryListing(a.id()));
        assertEmpty(mgr.directoryListing(b.id()));

        mgr.softDelete(path("/f1"), false, null);

        assertEquals(F.asMap("a", new IgfsListingEntry(a)), mgr.directoryListing(ROOT_ID));

        assertEmpty(mgr.directoryListing(a.id()));
        assertEmpty(mgr.directoryListing(b.id()));

        mgr.softDelete(path("/a"), false, null);

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
                return mgr.updateProperties(fileId, props);
            }
        }, cls, msg);
    }

    /**
     * Test expected failures for 'move file' operation.
     */
    private void expectsRenameFail(final String src, final String dst) {
        Throwable err = assertThrowsInherited(log, new Callable() {
            @Override public Object call() throws Exception {
                mgr.move(new IgfsPath(src), new IgfsPath(dst));

                return null;
            }
        }, IgfsException.class, null);

        assertTrue("Unexpected cause: " + err, err instanceof IgfsException);
    }
}
