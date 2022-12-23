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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
import org.apache.ignite.configuration.DiskPageCompression;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContextImpl;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIO;
import org.apache.ignite.internal.processors.cache.verify.IdleVerifyResultV2;
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
    protected static final long TIMEOUT = 30_000;

    /** */
    private static final Map<String, String> CACHES = new HashMap<>();

    static {
        CACHES.put("cache1", "group1");
        CACHES.put("cache2", "group1");
        CACHES.put("cache3", null);
        CACHES.put("cache4", null);
    }

    /** */
    private static final Set<String> COMPRESSED_CACHES = new HashSet<>();

    static {
        COMPRESSED_CACHES.add("cache1");
        COMPRESSED_CACHES.add("cache2");
        COMPRESSED_CACHES.add("cache3");
    }

    /** */
    @Parameterized.Parameters(name = "Encryption={0}")
    public static Collection<Boolean> encryptionParams() {
        return Collections.singletonList(false);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration config = super.getConfiguration(igniteInstanceName);

        config.getDataStorageConfiguration().setPageSize(PAGE_SIZE);

        return config;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();
        createTestSnapshot();
    }

    /** {@inheritDoc} */
    @Before
    @Override public void beforeTestSnapshot() {
        locEvts.clear();
    }

    /** {@inheritDoc} */
    @After
    @Override public void afterTestSnapshot() throws Exception {
        if (G.allGrids().isEmpty())
            return;

        IgniteEx ig = grid(0);
        for (String cacheName : ig.cacheNames()) {
            IgniteCache cache = ig.cache(cacheName);

            cache.destroy();
        }

        stopAllGrids();
    }

    /** */
    @Test
    public void testRestoreFullSnapshot() throws Exception {
        IgniteEx ignite = startGrids(3);
        ignite.events().localListen(e -> locEvts.add(e.type()), EVTS_CLUSTER_SNAPSHOT);
        ignite.cluster().state(ClusterState.ACTIVE);

        for (String snpName: Arrays.asList(SNAPSHOT_WITH_HOLES, SNAPSHOT_WITHOUT_HOLES)) {
            try {
                ignite.snapshot().restoreSnapshot(snpName, null).get(TIMEOUT);

                waitForEvents(EVT_CLUSTER_SNAPSHOT_RESTORE_STARTED, EVT_CLUSTER_SNAPSHOT_RESTORE_FINISHED);

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
        IgniteEx ignite = startGrids(3);
        ignite.events().localListen(e -> locEvts.add(e.type()), EVTS_CLUSTER_SNAPSHOT);
        ignite.cluster().state(ClusterState.ACTIVE);

        G.allGrids().forEach(this::failCompressionProcessor);

        for (String snpName: Arrays.asList(SNAPSHOT_WITH_HOLES, SNAPSHOT_WITHOUT_HOLES)) {
            GridTestUtils.assertThrows(log, () -> ignite.snapshot().restoreSnapshot(snpName, null).get(TIMEOUT),
                IgniteException.class, "Snapshot contains compressed cache groups");
        }
    }


    /** */
    @Test
    public void testRestoreNotCompressed_OnGridWithoutCompression() throws Exception {
        IgniteEx ignite = startGrids(3);
        ignite.events().localListen(e -> locEvts.add(e.type()), EVTS_CLUSTER_SNAPSHOT);
        ignite.cluster().state(ClusterState.ACTIVE);

        G.allGrids().forEach(i -> failCompressionProcessor(i));

        Collection<String> groupsWithoutCompression = CACHES.entrySet().stream()
            .filter(e -> !COMPRESSED_CACHES.contains(e.getKey()))
            .map(e -> e.getValue() != null ? e.getValue() : e.getKey())
            .distinct().collect(Collectors.toList());

        for (String snpName: Arrays.asList(SNAPSHOT_WITH_HOLES, SNAPSHOT_WITHOUT_HOLES)) {
            try {
                ignite.snapshot().restoreSnapshot(snpName, groupsWithoutCompression).get(TIMEOUT);

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

    /** */
    protected void createTestSnapshot() throws Exception {
        CacheConfiguration[] caches = CACHES.entrySet().stream()
            .map(cache -> {
                CacheConfiguration config = new CacheConfiguration(cache.getKey());

                config.setQueryEntities(Collections.singletonList(
                    new QueryEntity()
                        .setKeyType(Integer.class.getName())
                        .setValueType(Value.class.getName())
                        .addQueryField("id", Integer.class.getName(), null)
                        .addQueryField("name", String.class.getName(), null)
                        .setIndexes(F.asList(new QueryIndex("name")))
                ));

                if (cache.getValue() != null)
                    config.setGroupName(cache.getValue());

                if (COMPRESSED_CACHES.contains(cache.getKey()))
                    config.setDiskPageCompression(DISK_PAGE_COMPRESSION);
                else
                    config.setDiskPageCompression(DiskPageCompression.DISABLED);

                return config;
            }).toArray(CacheConfiguration[]::new);

        IgniteEx ignite = startGridsWithCache(3, 1000, valueBuilder(), caches);

        forceCheckpoint();

        G.allGrids().forEach(i -> failCompressionProcessor(i, SNAPSHOT_WITHOUT_HOLES));

        for (String snpName : Arrays.asList(SNAPSHOT_WITH_HOLES, SNAPSHOT_WITHOUT_HOLES)) {
            ignite.snapshot().createSnapshot(snpName).get(TIMEOUT);

            IdleVerifyResultV2 res = ignite.context().cache().context().snapshotMgr().checkSnapshot(snpName, null)
                .get().idleVerifyResult();

            StringBuilder b = new StringBuilder();
            res.print(b::append, true);

            assertTrue("Exceptions: " + b, F.isEmpty(res.exceptions()));
            assertTrue(F.isEmpty(res.exceptions()));
        }

        Path withHolesPath = Paths.get(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_SNAPSHOT_DIRECTORY, false)
            .toString(), SNAPSHOT_WITH_HOLES);

        Path withoutHolesPath = Paths.get(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_SNAPSHOT_DIRECTORY, false)
            .toString(), SNAPSHOT_WITHOUT_HOLES);

        long withHolesSize = directorySize(withHolesPath);
        long withoutHolesSize = directorySize(withoutHolesPath);

        assertTrue("withHolesSize < withoutHolesSize: " + withHolesSize + " < " + withoutHolesSize,
            withHolesSize < withoutHolesSize);

        long idxWithHolesSize = directorySize(withHolesPath, "index\\.bin");
        long idxWithoutHolesSize = directorySize(withoutHolesPath, "index\\.bin");

        assertTrue("idxWithHolesSize < idxWithoutHolesSize: " + idxWithHolesSize + " < " + idxWithoutHolesSize,
            idxWithHolesSize < idxWithoutHolesSize);

        ignite.cacheNames().forEach(c -> ignite.getOrCreateCache(c).destroy());

        G.stopAll(true);
    }

    /** */
    private void failCompressionProcessor(Ignite ignite, String... snpNames) {
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
    private static long directorySize(Path path) throws IOException {
        return directorySize(path, null);
    }

    /** */
    private static long directorySize(Path path, String pattern) throws IOException {
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
    }

    /** */
    private static class Value {
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

            Value value = (Value)o;

            return Objects.equals(name, value.name);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(name);
        }
    }
}
