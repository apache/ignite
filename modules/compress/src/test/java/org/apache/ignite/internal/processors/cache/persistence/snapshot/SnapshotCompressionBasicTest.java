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

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.DiskPageCompression;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContextImpl;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.management.cache.IdleVerifyResult;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIO;
import org.apache.ignite.internal.processors.cache.persistence.filename.IgniteDirectories;
import org.apache.ignite.internal.processors.compress.CompressionProcessor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import static org.apache.ignite.configuration.IgniteConfiguration.DFLT_SNAPSHOT_DIRECTORY;
import static org.apache.ignite.events.EventType.EVTS_CLUSTER_SNAPSHOT;
import static org.apache.ignite.events.EventType.EVT_CLUSTER_SNAPSHOT_RESTORE_FINISHED;
import static org.apache.ignite.events.EventType.EVT_CLUSTER_SNAPSHOT_RESTORE_STARTED;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;

/** */
public class SnapshotCompressionBasicTest extends AbstractSnapshotSelfTest {
    /** */
    protected static final DiskPageCompression DISK_PAGE_COMPRESSION = DiskPageCompression.SNAPPY;

    /** */
    protected static final int PAGE_SIZE = 8 * 1024;

    /** */
    protected static final String SNAPSHOT_WITHOUT_HOLES = "testSnapshotWithoutHoles";

    /** */
    protected static final String SNAPSHOT_WITH_HOLES = "testSnapshotWithHoles";

    /** */
    protected static final long TIMEOUT = 120_000;

    /** */
    protected static final Map<String, String> CACHES = new HashMap<>();

    /** */
    public static final int DFLT_GRIDS_CNT = 3;

    static {
        CACHES.put("cache1", "group1");
        CACHES.put("cache2", "group1");
        CACHES.put("cache3", null);
        CACHES.put("cache4", null);
    }

    /** */
    protected static final Set<String> COMPRESSED_CACHES = new HashSet<>();

    static {
        COMPRESSED_CACHES.add("cache1");
        COMPRESSED_CACHES.add("cache2");
        COMPRESSED_CACHES.add("cache3");
    }

    /** Parameters. */
    @Parameterized.Parameters(name = "encryption={0}, onlyPrimay={1}")
    public static Collection<Object[]> params() {
        List<Object[]> res = new ArrayList<>();

        for (boolean onlyPrimary: new boolean[] {true, false})
            res.add(new Object[] { false, onlyPrimary});

        return res;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.getDataStorageConfiguration().setPageSize(PAGE_SIZE);
        cfg.setWorkDirectory(workingDirectory(cfg).toString());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();
        createTestSnapshot();
    }

    /** {@inheritDoc} */
    @Before
    @Override public void beforeTestSnapshot() throws Exception {
        assertTrue(G.allGrids().isEmpty());

        locEvts.clear();

        cleanPersistenceDir(true);
    }

    /** {@inheritDoc} */
    @After
    @Override public void afterTestSnapshot() throws Exception {
        if (G.allGrids().isEmpty())
            return;

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override public void afterTestsStopped() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testRestoreFullSnapshot() throws Exception {
        testRestoreFullSnapshot(DFLT_GRIDS_CNT);
    }

    /** */
    @Test
    public void testRestoreFullSnapshot_OnLargerTopology() throws Exception {
        testRestoreFullSnapshot(2 * DFLT_GRIDS_CNT);
    }

    /** */
    private void testRestoreFullSnapshot(int gridCnt) throws Exception {
        IgniteEx ignite = startGrids(gridCnt);
        ignite.events().localListen(e -> locEvts.add(e.type()), EVTS_CLUSTER_SNAPSHOT);
        ignite.cluster().state(ClusterState.ACTIVE);

        long withoutHolesSize = snapshotSize(G.allGrids(), SNAPSHOT_WITHOUT_HOLES);

        for (String snpName: Arrays.asList(SNAPSHOT_WITH_HOLES, SNAPSHOT_WITHOUT_HOLES)) {
            try {
                ignite.snapshot().restoreSnapshot(snpName, null).get(TIMEOUT);

                waitForEvents(EVT_CLUSTER_SNAPSHOT_RESTORE_STARTED, EVT_CLUSTER_SNAPSHOT_RESTORE_FINISHED);

                long persistSz = persistenseSize(G.allGrids());

                assertTrue("persistSz < withoutHolesSize " + persistSz + "< " + withoutHolesSize,
                    persistSz < 0.75 * withoutHolesSize);

                for (String cacheName : CACHES.keySet()) {
                    IgniteCache cache = ignite.cache(cacheName);

                    assertCacheKeys(cache, 1000);

                    cache.destroy();
                }
            }
            finally {
                locEvts.clear();
            }
        }
    }

    /** */
    @Test
    public void testRestoreFail_OnGridWithoutCompression() throws Exception {
        IgniteEx ignite = startGrids(DFLT_GRIDS_CNT);
        ignite.events().localListen(e -> locEvts.add(e.type()), EVTS_CLUSTER_SNAPSHOT);
        ignite.cluster().state(ClusterState.ACTIVE);

        G.allGrids().forEach(this::failCompressionProcessor);

        for (String snpName : Arrays.asList(SNAPSHOT_WITH_HOLES, SNAPSHOT_WITHOUT_HOLES)) {
            GridTestUtils.assertThrowsAnyCause(
                log,
                () -> ignite.snapshot().restoreSnapshot(snpName, null).get(TIMEOUT),
                IllegalStateException.class,
                "from snapshot '" + snpName + "' are compressed while disk page compression is disabled. To check " +
                    "these groups please start Ignite with ignite-compress"
            );
        }
    }

    /** */
    @Test
    public void testRestoreNotCompressed_OnGridWithoutCompression() throws Exception {
        IgniteEx ignite = startGrids(DFLT_GRIDS_CNT);
        ignite.events().localListen(e -> locEvts.add(e.type()), EVTS_CLUSTER_SNAPSHOT);
        ignite.cluster().state(ClusterState.ACTIVE);

        G.allGrids().forEach(i -> failCompressionProcessor(i));

        Collection<String> grpsWithoutCompression = CACHES.entrySet().stream()
            .filter(e -> !COMPRESSED_CACHES.contains(e.getKey()))
            .map(e -> e.getValue() != null ? e.getValue() : e.getKey())
            .distinct().collect(Collectors.toList());

        for (String snpName: Arrays.asList(SNAPSHOT_WITH_HOLES, SNAPSHOT_WITHOUT_HOLES)) {
            try {
                ignite.snapshot().restoreSnapshot(snpName, grpsWithoutCompression).get(TIMEOUT);

                waitForEvents(EVT_CLUSTER_SNAPSHOT_RESTORE_STARTED, EVT_CLUSTER_SNAPSHOT_RESTORE_FINISHED);

                CACHES.keySet().stream().filter(c -> !COMPRESSED_CACHES.contains(c)).forEach(cacheName -> {
                    IgniteCache cache = ignite.cache(cacheName);

                    assertCacheKeys(cache, 1000);

                    cache.destroy();
                });
            }
            finally {
                locEvts.clear();
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected Function<Integer, Object> valueBuilder() {
        return i -> new Value("name_" + i);
    }

    /** {@inheritDoc} */
    @Override protected void cleanPersistenceDir() throws Exception {
        super.cleanPersistenceDir();

        cleanPersistenceDir(false);
    }

    /** */
    @Override protected void cleanPersistenceDir(boolean saveSnap) throws Exception {
        assertTrue("Grids are not stopped", F.isEmpty(G.allGrids()));

        String mask = U.maskForFileName(getTestIgniteInstanceName());

        try (DirectoryStream<Path> ds = Files.newDirectoryStream(defaultWorkDirectory(),
            path -> Files.isDirectory(path) && path.getFileName().toString().contains(mask))
        ) {
            for (Path dir : ds) {
                if (!saveSnap) {
                    U.delete(dir);

                    continue;
                }

                U.delete(U.resolveWorkDirectory(dir.toString(), "cp", false));
                U.delete(U.resolveWorkDirectory(dir.toString(), DFLT_STORE_DIR, false));

                IgniteDirectories dirs = new IgniteDirectories(dir.toString());

                U.delete(dirs.marshaller());
                U.delete(dirs.binaryMetaRoot());
            }
        }
        catch (IOException e) {
            throw new IgniteException(e);
        }
    }

    /** */
    protected void createTestSnapshot() throws Exception {
        CacheConfiguration[] caches = CACHES.entrySet().stream()
            .map(cache -> {
                CacheConfiguration cfg = new CacheConfiguration(cache.getKey());

                cfg.setQueryEntities(Collections.singletonList(
                    new QueryEntity()
                        .setKeyType(Integer.class.getName())
                        .setValueType(Value.class.getName())
                        .addQueryField("id", Integer.class.getName(), null)
                        .addQueryField("name", String.class.getName(), null)
                        .setIndexes(F.asList(new QueryIndex("name")))
                ));

                if (cache.getValue() != null)
                    cfg.setGroupName(cache.getValue());

                if (COMPRESSED_CACHES.contains(cache.getKey()))
                    cfg.setDiskPageCompression(DISK_PAGE_COMPRESSION);
                else
                    cfg.setDiskPageCompression(DiskPageCompression.DISABLED);

                return cfg;
            }).toArray(CacheConfiguration[]::new);

        IgniteEx ignite = startGridsWithCache(DFLT_GRIDS_CNT, 1000, valueBuilder(), caches);

        forceCheckpoint();

        G.allGrids().forEach(i -> failCompressionProcessor(i, SNAPSHOT_WITHOUT_HOLES));

        for (String snpName : Arrays.asList(SNAPSHOT_WITH_HOLES, SNAPSHOT_WITHOUT_HOLES)) {
            snp(ignite).createSnapshot(snpName, null, false, onlyPrimary).get(TIMEOUT);

            IdleVerifyResult res = ignite.context().cache().context().snapshotMgr().checkSnapshot(snpName, null)
                .get().idleVerifyResult();

            StringBuilder b = new StringBuilder();
            res.print(b::append, true);

            assertTrue("Exceptions: " + b, F.isEmpty(res.exceptions()));
            assertTrue(F.isEmpty(res.exceptions()));
        }

        long withHolesSize = snapshotSize(G.allGrids(), SNAPSHOT_WITH_HOLES);
        long withoutHolesSize = snapshotSize(G.allGrids(), SNAPSHOT_WITHOUT_HOLES);

        assertTrue("withHolesSize < withoutHolesSize: " + withHolesSize + " < " + withoutHolesSize,
            withHolesSize < withoutHolesSize);

        long idxWithHolesSize = snapshotSize(G.allGrids(), SNAPSHOT_WITH_HOLES, "index\\.bin");
        long idxWithoutHolesSize = snapshotSize(G.allGrids(), SNAPSHOT_WITHOUT_HOLES, "index\\.bin");

        assertTrue("idxWithHolesSize < idxWithoutHolesSize: " + idxWithHolesSize + " < " + idxWithoutHolesSize,
            idxWithHolesSize < idxWithoutHolesSize);

        G.stopAll(true);
    }

    /** */
    protected void failCompressionProcessor(Ignite ignite, String... snpNames) {
        CompressionProcessor compressProc = ((IgniteEx)ignite).context().compress();

        CompressionProcessor spyCompressProc = Mockito.spy(compressProc);

        if (F.isEmpty(snpNames)) {
            try {
                Mockito.doAnswer(inv -> {
                    throw new IgniteCheckedException(new IgniteException("errno: -12"));
                }).when(spyCompressProc).checkPageCompressionSupported();

                Mockito.doAnswer(inv -> {
                    throw new IgniteCheckedException(new IgniteException("errno: -12"));
                }).when(spyCompressProc).checkPageCompressionSupported(Mockito.any(), Mockito.anyInt());
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }
        else {
            for (String snpName : snpNames) {
                try {
                    Mockito.doAnswer(inv -> {
                        if (snpName != null && ((Path)inv.getArgument(0)).endsWith(snpName))
                            throw new IgniteCheckedException(new IgniteException("errno: -12"));
                        return null;
                    }).when(spyCompressProc).checkPageCompressionSupported(Mockito.any(), Mockito.anyInt());
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
            }
        }

        ((GridKernalContextImpl)((IgniteEx)ignite).context()).add(spyCompressProc);
    }

    /** */
    protected long persistenseSize(Collection<Ignite> grids) {
        return grids.stream()
            .map(ig -> workingDirectory(ig).resolve(DFLT_STORE_DIR))
            .reduce(0L, (acc, p) -> acc + directorySize(p), Long::sum);
    }

    /** */
    protected long snapshotSize(Collection<Ignite> grids, String snpName) {
        return snapshotSize(grids, snpName, "(part-\\d+|index)\\.bin");
    }

    /** */
    protected long snapshotSize(Collection<Ignite> grids, String snpName, String pattern) {
        return grids.stream()
            .map(ig -> workingDirectory(ig).resolve(DFLT_SNAPSHOT_DIRECTORY).resolve(snpName))
            .reduce(0L, (acc, p) -> acc + directorySize(p, pattern), Long::sum);
    }

    /** */
    protected long directorySize(Path path) {
        return directorySize(path, "(part-\\d+|index)\\.bin");
    }

    /** */
    protected long directorySize(Path path, String pattern) {
        if (!Files.exists(path))
            return 0;

        try (Stream<Path> walk = Files.walk(path)) {
            return walk.filter(Files::isRegularFile)
                .filter(f -> F.isEmpty(pattern) || f.getFileName().toString().matches(pattern))
                .mapToLong(p -> {
                    try (FileIO fio = new RandomAccessFileIO(p.toFile(), StandardOpenOption.READ)) {
                        return fio.getSparseSize();
                    }
                    catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }).sum();
        }
        catch (IOException e) {
            throw new IgniteException(e);
        }
    }

    /** */
    protected Path workingDirectory(Ignite ig) {
        return workingDirectory(ig.configuration());
    }

    /** */
    protected Path workingDirectory(IgniteConfiguration cfg) {
        try {
            return Paths.get(U.defaultWorkDirectory(), U.maskForFileName(cfg.getIgniteInstanceName()));
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** */
    protected Path defaultWorkDirectory() {
        try {
            return Paths.get(U.defaultWorkDirectory());
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** */
    static class Value {
        /** */
        String name;

        /** */
        Value(String name) {
            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Value val = (Value)o;

            return Objects.equals(name, val.name);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(name);
        }
    }
}
