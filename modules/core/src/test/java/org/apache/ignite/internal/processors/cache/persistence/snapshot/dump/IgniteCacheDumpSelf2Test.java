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

package org.apache.ignite.internal.processors.cache.persistence.snapshot.dump;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.zip.ZipInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntryVersion;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cdc.TypeMapping;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.dump.DumpConsumer;
import org.apache.ignite.dump.DumpEntry;
import org.apache.ignite.dump.DumpReader;
import org.apache.ignite.dump.DumpReaderConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheConflictResolutionManager;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.processors.cache.CacheObjectValueContext;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheManagerAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.processors.cache.dr.GridCacheDrInfo;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.dump.AbstractCacheDumpTest.TestDumpConsumer;
import org.apache.ignite.internal.processors.cache.version.CacheVersionConflictResolver;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionConflictContext;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionManager;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionedEntryEx;
import org.apache.ignite.internal.processors.cacheobject.UserCacheObjectImpl;
import org.apache.ignite.internal.processors.dr.GridDrType;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.platform.model.User;
import org.apache.ignite.plugin.AbstractCachePluginProvider;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.CachePluginContext;
import org.apache.ignite.plugin.CachePluginProvider;
import org.apache.ignite.spi.encryption.EncryptionSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.apache.ignite.configuration.IgniteConfiguration.DFLT_SNAPSHOT_DIRECTORY;
import static org.apache.ignite.dump.DumpReaderConfiguration.DFLT_THREAD_CNT;
import static org.apache.ignite.dump.DumpReaderConfiguration.DFLT_TIMEOUT;
import static org.apache.ignite.internal.encryption.AbstractEncryptionTest.MASTER_KEY_NAME_2;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_DATA_FILENAME;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_DIR_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.PART_FILE_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.ZIP_SUFFIX;
import static org.apache.ignite.internal.processors.cache.persistence.filename.PdsFolderResolver.DB_DEFAULT_FOLDER;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.DFLT_SNAPSHOT_TMP_DIR;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.DUMP_LOCK;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.SNAPSHOT_TRANSFER_RATE_DMS_KEY;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.dump.AbstractCacheDumpTest.CACHE_0;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.dump.AbstractCacheDumpTest.DMP_NAME;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.dump.AbstractCacheDumpTest.KEYS_CNT;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.dump.AbstractCacheDumpTest.USER_FACTORY;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.dump.AbstractCacheDumpTest.dump;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.dump.AbstractCacheDumpTest.dumpDirectory;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.dump.AbstractCacheDumpTest.encryptionSpi;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.dump.AbstractCacheDumpTest.invokeCheckCommand;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.dump.CreateDumpFutureTask.DUMP_FILE_EXT;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.dump.DumpEntrySerializer.HEADER_SZ;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/** */
public class IgniteCacheDumpSelf2Test extends GridCommonAbstractTest {
    /** */
    private LogListener lsnr;

    /** */
    private boolean persistence;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (lsnr != null) {
            ListeningTestLogger testLog = new ListeningTestLogger(log);

            testLog.registerListener(lsnr);

            cfg.setGridLogger(testLog);
        }

        if (persistence) {
            cfg.setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true)));
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** Checks a dump when it is created with the data streamer just after a restart. */
    @Test
    public void testDumpAfterRestartWithStreamer() throws Exception {
        doTestDumpAfterRestart(true);
    }

    /** Checks a dump when it is created just after a restart. */
    @Test
    public void testDumpAfterRestart() throws Exception {
        doTestDumpAfterRestart(false);
    }

    /** Doest dump test when it is created just after restart. */
    private void doTestDumpAfterRestart(boolean useDataStreamer) throws Exception {
        persistence = true;

        int nodes = 2;

        IgniteEx ign0 = startGrids(nodes);

        ign0.cluster().state(ClusterState.ACTIVE);

        ign0.createCache(defaultCacheConfiguration());

        try (IgniteDataStreamer<Integer, String> ds = ign0.dataStreamer(DEFAULT_CACHE_NAME)) {
            IgniteCache<Integer, String> cache = ign0.cache(DEFAULT_CACHE_NAME);

            for (int i = 0; i < KEYS_CNT; ++i) {
                if (useDataStreamer)
                    ds.addData(i, "" + i);
                else
                    cache.put(i, "" + i);
            }
        }

        stopAllGrids(false);
        IgniteEx ign1 = startGrids(nodes);
        ign1.cluster().state(ClusterState.ACTIVE);

        ign1.snapshot().createDump(DMP_NAME, Collections.singletonList(DEFAULT_CACHE_NAME)).get(getTestTimeout());

        ign1.destroyCache(DEFAULT_CACHE_NAME);

        new DumpReader(new DumpReaderConfiguration(dumpDirectory(ign1, DMP_NAME), new DumpConsumer() {
            @Override public void start() {
                // No-op.
            }

            @Override public void onMappings(Iterator<TypeMapping> mappings) {
                // No-op.
            }

            @Override public void onTypes(Iterator<BinaryType> types) {
                // No-op.
            }

            @Override public void onCacheConfigs(Iterator<StoredCacheData> caches) {
                caches.forEachRemaining(cacheData -> ign1.createCache(cacheData.config()));
            }

            @Override public void onPartition(int grp, int part, Iterator<DumpEntry> data) {
                data.forEachRemaining(de ->
                    ign1.cache(ign1.context().cache().cacheDescriptor(de.cacheId()).cacheName()).put(de.key(), de.value())
                );
            }

            @Override public void stop() {
                // No-op.
            }
        }), log).run();

        IgniteCache<Integer, String> cache = ign1.cache(DEFAULT_CACHE_NAME);

        assertNotNull(cache);
        assertEquals(KEYS_CNT, cache.size());

        for (int i = 0; i < KEYS_CNT; ++i)
            assertEquals(i + "", cache.get(i));
    }

    /** */
    @Test
    public void testSnapshotDirectoryCreatedLazily() throws Exception {
        try (IgniteEx ign = startGrid(new IgniteConfiguration())) {
            File snpDir = new File(U.defaultWorkDirectory(), DFLT_SNAPSHOT_DIRECTORY);
            File tmpSnpDir = new File(
                ign.context().pdsFolderResolver().resolveFolders().persistentStoreNodePath().getAbsolutePath(),
                DFLT_SNAPSHOT_TMP_DIR
            );

            assertFalse(snpDir + " must created lazily for in-memory node", snpDir.exists());
            assertFalse(tmpSnpDir + " must created lazily for in-memory node", tmpSnpDir.exists());
        }
    }

    /** */
    @Test
    public void testDumpFailIfNoCaches() throws Exception {
        try (IgniteEx ign = startGrid(new IgniteConfiguration())) {
            ign.cluster().state(ClusterState.ACTIVE);

            assertThrows(
                null,
                () -> ign.snapshot().createDump("dump", null).get(),
                IgniteException.class,
                "Dump operation has been rejected. No cache group defined in cluster"
            );
        }
    }

    /** */
    @Test
    public void testUnreadyDumpCleared() throws Exception {
        IgniteEx ign = (IgniteEx)startGridsMultiThreaded(2);

        ign.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Integer, Integer> cache = ign.createCache(DEFAULT_CACHE_NAME);

        IntStream.range(0, KEYS_CNT).forEach(i -> cache.put(i, i));

        ign.snapshot().createDump(DMP_NAME, null).get(getTestTimeout());

        stopAllGrids();

        Dump dump = dump(ign, DMP_NAME);

        List<String> nodes = dump.nodesDirectories();

        assertNotNull(nodes);
        assertEquals(2, nodes.size());

        File nodeDumpDir = new File(dump.dumpDirectory(), DB_DEFAULT_FOLDER + File.separator + nodes.get(0));

        assertTrue(new File(nodeDumpDir, DUMP_LOCK).createNewFile());

        lsnr = LogListener.matches("Found locked dump dir. " +
            "This means, dump creation not finished prior to node fail. " +
            "Directory will be deleted: " + nodeDumpDir.getAbsolutePath()).build();

        startGridsMultiThreaded(2);

        assertFalse(nodeDumpDir.exists());
        assertTrue(lsnr.check());
    }

    /** */
    @Test
    public void testDumpIteratorFaileOnWrongCrc() throws Exception {
        try (IgniteEx ign = startGrid(new IgniteConfiguration())) {
            ign.cluster().state(ClusterState.ACTIVE);

            IgniteCache<Integer, Integer> cache = ign.createCache(DEFAULT_CACHE_NAME);

            for (int key : partitionKeys(cache, 0, KEYS_CNT, 0))
                cache.put(key, key);

            ign.snapshot().createDump(DMP_NAME, null).get();

            Dump dump = dump(ign, DMP_NAME);

            List<String> nodes = dump.nodesDirectories();

            assertNotNull(nodes);
            assertEquals(1, nodes.size());

            File cacheDumpDir = new File(
                dump.dumpDirectory(),
                DB_DEFAULT_FOLDER + File.separator + nodes.get(0) + File.separator + CACHE_DIR_PREFIX + DEFAULT_CACHE_NAME
            );

            assertTrue(cacheDumpDir.exists());

            Set<File> dumpFiles = Arrays.asList(cacheDumpDir.listFiles()).stream()
                .filter(f -> {
                    try {
                        return Files.size(f.toPath()) > 0;
                    }
                    catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(Collectors.toSet());

            String partDumpName = PART_FILE_PREFIX + 0 + DUMP_FILE_EXT;

            assertTrue(dumpFiles.stream().anyMatch(f -> f.getName().equals(CACHE_DATA_FILENAME)));
            assertTrue(dumpFiles.stream().anyMatch(f -> f.getName().equals(partDumpName)));

            try (FileChannel fc = FileChannel.open(Paths.get(cacheDumpDir.getAbsolutePath(), partDumpName), READ, WRITE)) {
                fc.position(HEADER_SZ); // Skip first entry header.

                int bufSz = 5;

                ByteBuffer buf = ByteBuffer.allocate(bufSz);

                assertEquals(bufSz, fc.read(buf));

                buf.position(0);

                // Increment first five bytes in dumped entry.
                for (int i = 0; i < bufSz; i++) {
                    byte b = buf.get();
                    b++;
                    buf.position(i);
                    buf.put(b);
                }

                fc.position(HEADER_SZ);

                buf.rewind();
                fc.write(buf);
            }

            assertThrows(
                null,
                () -> dump.iterator(nodes.get(0), CU.cacheId(DEFAULT_CACHE_NAME), 0).next(),
                IgniteException.class,
                "Data corrupted"
            );
        }
    }

    /** */
    @Test
    public void testCheckFailOnCorruptedData() throws Exception {
        IgniteEx ign = (IgniteEx)startGridsMultiThreaded(2);

        IgniteCache<Integer, Integer> cache = ign.createCache(new CacheConfiguration<Integer, Integer>()
            .setName(DEFAULT_CACHE_NAME)
            .setBackups(1)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC));

        IntStream.range(0, KEYS_CNT).forEach(i -> cache.put(i, i));

        int corruptedPart = 1;
        int corruptedKey = partitionKeys(cache, corruptedPart, 1, 0).get(0);

        cache.put(corruptedKey, corruptedKey);

        IgniteInternalCache<Integer, Object> cachex = ign.cachex(DEFAULT_CACHE_NAME);

        GridCacheVersionManager mgr = cachex.context().shared().versions();

        GridCacheAdapter<Integer, Integer> adapter = (GridCacheAdapter<Integer, Integer>)cachex.<Integer, Integer>cache();

        GridCacheEntryEx entry = adapter.entryEx(corruptedKey);

        entry.innerUpdate(
            mgr.next(entry.context().kernalContext().discovery().topologyVersion()),
            ign.localNode().id(),
            ign.localNode().id(),
            GridCacheOperation.UPDATE,
            new UserCacheObjectImpl(corruptedKey + 1, null),
            null,
            false,
            false,
            false,
            false,
            null,
            false,
            false,
            false,
            false,
            false,
            AffinityTopologyVersion.NONE,
            null,
            GridDrType.DR_NONE,
            0,
            0,
            null,
            false,
            false,
            null,
            null,
            null,
            null,
            false);

        ign.snapshot().createDump(DMP_NAME, null).get();

        String out = invokeCheckCommand(ign, DMP_NAME);

        assertContains(
            null,
            out,
            "Conflict partition: PartitionKeyV2 [grpId=" + CU.cacheId(DEFAULT_CACHE_NAME) +
                ", grpName=" + DEFAULT_CACHE_NAME +
                ", partId=" + corruptedPart + "]"
        );

        String verPattern = "partVerHash=(-)?[0-9]+";
        String hashPattern = "partHash=(-)?[0-9]+";

        Matcher m = Pattern.compile(verPattern).matcher(out);

        assertTrue(m.find());
        String ver0 = out.substring(m.start(), m.end());

        assertTrue(m.find());
        String ver1 = out.substring(m.start(), m.end());

        assertFalse(m.find());

        m = Pattern.compile(hashPattern).matcher(out);

        assertTrue(m.find());
        String hash0 = out.substring(m.start(), m.end());

        assertTrue(m.find());
        String hash1 = out.substring(m.start(), m.end());

        assertFalse(m.find());

        assertFalse(Objects.equals(ver0, ver1));
        assertFalse(Objects.equals(hash0, hash1));
    }

    /** */
    @Test
    public void testCustomLocation() throws Exception {
        try (IgniteEx ign = startGrid()) {
            IgniteCache<Integer, Integer> cache = ign.createCache(new CacheConfiguration<Integer, Integer>()
                .setName("test-cache-0")
                .setBackups(1)
                .setAtomicityMode(CacheAtomicityMode.ATOMIC));

            IntStream.range(0, KEYS_CNT).forEach(i -> cache.put(i, i));

            File snpDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), "ex_snapshots", true);

            assertTrue(U.delete(snpDir));

            ign.context().cache().context().snapshotMgr().createSnapshot(
                DMP_NAME,
                snpDir.getAbsolutePath(),
                null,
                false,
                false,
                true,
                false,
                false
            ).get();

            assertFalse(
                "Standard snapshot directory must created lazily for in-memory node",
                new File(U.defaultWorkDirectory(), DFLT_SNAPSHOT_DIRECTORY).exists()
            );

            assertFalse(
                "Temporary snapshot directory must created lazily for in-memory node",
                new File(
                    ign.context().pdsFolderResolver().resolveFolders().persistentStoreNodePath().getAbsolutePath(),
                    DFLT_SNAPSHOT_TMP_DIR
                ).exists()
            );

            assertTrue(snpDir.exists());

            assertEquals(
                "The check procedure has finished, no conflicts have been found.\n\n",
                invokeCheckCommand(ign, DMP_NAME, snpDir.getAbsolutePath())
            );
        }
    }

    /** */
    @Test
    public void testCheckOnEmptyNode() throws Exception {
        String id = "test";

        IgniteEx ign = startGrid(getConfiguration(id).setConsistentId(id));

        IgniteCache<Integer, Integer> cache = ign.createCache(new CacheConfiguration<Integer, Integer>()
            .setName("test-cache-0")
            .setBackups(1)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC));

        IgniteCache<Integer, User> cache2 = ign.createCache(new CacheConfiguration<Integer, User>()
            .setName("users")
            .setBackups(1)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC));

        IntStream.range(0, KEYS_CNT).forEach(i -> {
            cache.put(i, i);
            cache2.put(i, USER_FACTORY.apply(i));
        });

        ign.snapshot().createDump(DMP_NAME, null).get();

        assertEquals("The check procedure has finished, no conflicts have been found.\n\n", invokeCheckCommand(ign, DMP_NAME));

        stopAllGrids();

        cleanPersistenceDir(true);

        ListeningTestLogger testLog = new ListeningTestLogger(log);

        LogListener lsnr = LogListener.matches("Unknown cache groups will not be included in snapshot").build();

        testLog.registerListener(lsnr);

        ign = startGrid(getConfiguration(id).setConsistentId(id).setGridLogger(testLog));

        assertEquals("The check procedure has finished, no conflicts have been found.\n\n", invokeCheckCommand(ign, DMP_NAME));

        ign.createCache(DEFAULT_CACHE_NAME).put(1, 1);

        ign.snapshot().createDump(DMP_NAME + "2", Arrays.asList(DEFAULT_CACHE_NAME, "non-existing-group")).get();

        assertTrue(lsnr.check());
    }

    /** */
    @Test
    public void testCompareRawWithCompressedCacheDumps() throws Exception {
        String id = "test";

        IgniteEx ign = startGrid(getConfiguration(id).setConsistentId(id));

        int parts = 20;

        IgniteCache<Integer, Integer> cache = ign.createCache(new CacheConfiguration<Integer, Integer>()
            .setName(CACHE_0)
            .setAffinity(new RendezvousAffinityFunction().setPartitions(parts))
        );

        IntStream.range(0, KEYS_CNT).forEach(i -> cache.put(i, i));

        String rawDump = "rawDump";
        String zipDump = "zipDump";

        ign.context().cache().context().snapshotMgr()
            .createSnapshot(rawDump, null, null, false, true, true, false, false).get();

        ign.context().cache().context().snapshotMgr()
            .createSnapshot(zipDump, null, null, false, true, true, true, false).get();

        stopAllGrids();

        Map<Integer, Long> rawSizes = Arrays
            .stream(new File(dumpDirectory(ign, rawDump) + "/db/" + id + "/cache-" + CACHE_0).listFiles())
            .filter(f -> !f.getName().equals("cache_data.dat"))
            .peek(f -> assertTrue(f.getName().startsWith(PART_FILE_PREFIX) && f.getName().endsWith(DUMP_FILE_EXT)))
            .collect(Collectors.toMap(
                f -> Integer.parseInt(f.getName().substring(PART_FILE_PREFIX.length(), f.getName().length() - DUMP_FILE_EXT.length())),
                File::length
            ));

        Map<Integer, Long> zipSizes = Arrays
            .stream(new File(dumpDirectory(ign, zipDump) + "/db/" + id + "/cache-" + CACHE_0).listFiles())
            .filter(f -> !f.getName().equals("cache_data.dat"))
            .peek(f -> assertTrue(f.getName().startsWith(PART_FILE_PREFIX) && f.getName().endsWith(DUMP_FILE_EXT + ZIP_SUFFIX)))
            .collect(Collectors.toMap(
                f -> Integer.parseInt(f.getName().substring(PART_FILE_PREFIX.length(),
                    f.getName().length() - (DUMP_FILE_EXT + ZIP_SUFFIX).length())
                ),
                File::length
            ));

        assertEquals(parts, rawSizes.keySet().size());

        assertEquals("Different set of partitions", rawSizes.keySet(), zipSizes.keySet());

        zipSizes.keySet().forEach( p -> assertTrue("Compressed partition " + p + " file size should not be zero", zipSizes.get(p) > 0));

        rawSizes.keySet().forEach( p ->
            assertTrue("Compressed size " + zipSizes.get(p) + " should be smaller than raw size " + rawSizes.get(p),
                rawSizes.get(p) > zipSizes.get(p)
            )
        );

        IntStream.range(0, parts).forEach(i -> {
            try {
                String entryName = PART_FILE_PREFIX + i + DUMP_FILE_EXT;

                File rawFile = new File(dumpDirectory(ign, rawDump) + "/db/" + id + "/cache-" + CACHE_0 + "/" + entryName);
                File zipFile = new File(dumpDirectory(ign, zipDump) + "/db/" + id + "/cache-" + CACHE_0 + "/" + entryName + ZIP_SUFFIX);

                byte[] rawFileContent = Files.readAllBytes(rawFile.toPath());

                ZipInputStream zis = new ZipInputStream(new FileInputStream(zipFile));

                assertEquals(entryName, zis.getNextEntry().getName());

                byte[] zipFileContent = IOUtils.toByteArray(zis);

                assertEqualsArraysAware("Files should have same data " + rawFile + " and " + zipFile, rawFileContent, zipFileContent);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    /** */
    @Test
    public void testDumpEntryConflictVersion() throws Exception {
        IgniteConfiguration cfg = getConfiguration("test").setPluginProviders(new AbstractTestPluginProvider() {
            @Override public String name() {
                return "ConflictResolverProvider";
            }

            @Override public CachePluginProvider createCacheProvider(CachePluginContext ctx) {
                if (!ctx.igniteCacheConfiguration().getName().equals(DEFAULT_CACHE_NAME))
                    return null;

                return new AbstractCachePluginProvider() {
                    @Override public @Nullable Object createComponent(Class cls) {
                        if (cls != CacheConflictResolutionManager.class)
                            return null;

                        return new TestCacheConflictResolutionManager();
                    }
                };
            }
        });

        IgniteEx ign = startGrid(cfg);

        IgniteCache<Integer, Integer> cache = ign.createCache(new CacheConfiguration<Integer, Integer>()
            .setName(DEFAULT_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction().setPartitions(3))
        );

        int topVer = 42;
        int dataCenterId = 31;
        int nodeOrder = 13;

        Map<KeyCacheObject, GridCacheDrInfo> drMap = new HashMap<>();

        IgniteInternalCache<Integer, Integer> intCache = ign.cachex(cache.getName());

        for (int i = 0; i < KEYS_CNT; i++) {
            KeyCacheObject key = new KeyCacheObjectImpl(i, null, intCache.affinity().partition(i));
            CacheObject val = new CacheObjectImpl(i, null);

            val.prepareMarshal(intCache.context().cacheObjectContext());

            drMap.put(key, new GridCacheDrInfo(val, new GridCacheVersion(topVer, i, nodeOrder, dataCenterId)));
        }

        intCache.putAllConflict(drMap);

        ign.snapshot().createDump(DMP_NAME, null).get(getTestTimeout());

        TestDumpConsumer cnsmr = new TestDumpConsumer() {
            @Override public void onPartition(int grp, int part, Iterator<DumpEntry> data) {
                data.forEachRemaining(e -> {
                    int key = (int)e.key();
                    int val = (int)e.value();

                    assertNotNull(e.version());

                    CacheEntryVersion conflictVer = e.version().otherClusterVersion();

                    assertNotNull(conflictVer);
                    assertEquals(topVer, conflictVer.topologyVersion());
                    assertEquals(nodeOrder, conflictVer.nodeOrder());
                    assertEquals(dataCenterId, conflictVer.clusterId());
                    assertEquals(key, val);
                    assertEquals(key, conflictVer.order());
                });
            }
        };

        new DumpReader(
            new DumpReaderConfiguration(
                dumpDirectory(ign, DMP_NAME),
                cnsmr,
                DFLT_THREAD_CNT,
                DFLT_TIMEOUT,
                true,
                false,
                null,
                false,
                null
            ),
            log
        ).run();

        cnsmr.check();
    }

    /** */
    @Test
    public void testCreateEncryptedFail() throws Exception {
        BiConsumer<IgniteEx, String> check = (ign, msg) -> assertThrows(null, () -> {
            ign.context().cache().context().snapshotMgr()
                .createSnapshot(DMP_NAME, null, null, false, false, true, false, true).get(getTestTimeout());
        }, IgniteException.class, msg);

        try (IgniteEx srv = startGrid()) {
            IgniteCache<Integer, Integer> cache = srv.createCache(DEFAULT_CACHE_NAME);
            IntStream.range(0, KEYS_CNT).forEach(i -> cache.put(i, i));

            IgniteEx cli = startClientGrid(1);

            check.accept(srv, "You have to configure custom EncryptionSpi implementation");
            check.accept(cli, "Snapshot has not been created");
        }
    }

    /** */
    @Test
    public void testReadEncrypted() throws Exception {
        File dumpDir;

        try (IgniteEx srv = startGrid(new IgniteConfiguration().setEncryptionSpi(encryptionSpi()))) {
            IgniteCache<Integer, byte[]> cache = srv.createCache(DEFAULT_CACHE_NAME);
            IntStream.range(0, KEYS_CNT).forEach(i -> {
                byte[] data = new byte[Math.max(Integer.BYTES, ThreadLocalRandom.current().nextInt((int)U.KB))];

                U.intToBytes(i, data, 0);

                cache.put(i, data);
            });

            srv.context().cache().context().snapshotMgr()
                .createSnapshot(DMP_NAME, null, null, false, false, true, false, true).get(getTestTimeout());

            dumpDir = dumpDirectory(srv, DMP_NAME);
        }

        assertThrows(null, () -> new DumpReader(
            new DumpReaderConfiguration(
                dumpDir,
                new TestDumpConsumer() {
                    @Override public void onPartition(int grp, int part, Iterator<DumpEntry> data) {
                        data.forEachRemaining(e -> {
                            assert e != null;
                        });
                    }
                },
                DFLT_THREAD_CNT,
                DFLT_TIMEOUT,
                true,
                false,
                null,
                false,
                null
            ),
            log
        ).run(), IgniteException.class, "Encryption SPI required to read encrypted dump");

        assertThrows(
            null,
            () -> {
                EncryptionSpi encSpi = encryptionSpi();

                encSpi.setMasterKeyName(MASTER_KEY_NAME_2);

                new DumpReader(
                    new DumpReaderConfiguration(
                        dumpDir,
                        new TestDumpConsumer() {
                            @Override public void onPartition(int grp, int part, Iterator<DumpEntry> data) {
                                data.forEachRemaining(e -> {
                                    assert e != null;
                                });
                            }
                        },
                        DFLT_THREAD_CNT,
                        DFLT_TIMEOUT,
                        true,
                        false,
                        null,
                        false,
                        encSpi
                    ),
                    log
                ).run();
            },
            IgniteException.class,
            "Dump '" + DMP_NAME + "' has different master key digest"
        );

        Map<Integer, Integer> dumpEntries = new HashMap<>();

        TestDumpConsumer cnsmr = new TestDumpConsumer() {
            @Override public void onPartition(int grp, int part, Iterator<DumpEntry> data) {
                data.forEachRemaining(e -> {
                    int v = U.bytesToInt((byte[])e.value(), 0);

                    dumpEntries.put((Integer)e.key(), v);
                });
            }
        };

        new DumpReader(
            new DumpReaderConfiguration(
                dumpDir,
                cnsmr,
                DFLT_THREAD_CNT,
                DFLT_TIMEOUT,
                true,
                false,
                null,
                false,
                encryptionSpi()
            ),
            log
        ).run();

        cnsmr.check();

        IntStream.range(0, KEYS_CNT).forEach(i -> assertEquals((Integer)i, dumpEntries.get(i)));
    }

    /** */
    @Test
    public void testDumpRateLimiter() throws Exception {
        try (IgniteEx ign = startGrid(0)) {
            ign.cluster().state(ClusterState.ACTIVE);

            byte[] val = new byte[(int)U.KB];
            ThreadLocalRandom.current().nextBytes(val);

            int keysCnt = 10;

            for (int i = 0; i < keysCnt; i++)
                ign.getOrCreateCache(DEFAULT_CACHE_NAME).put(i, val);

            ign.context().distributedConfiguration()
                .property(SNAPSHOT_TRANSFER_RATE_DMS_KEY)
                .propagate(U.KB);

            IgniteFuture<Void> fut = ign.snapshot().createDump(DMP_NAME, null);

            IgniteSnapshotManager snpMgr = ign.context().cache().context().snapshotMgr();

            assertTrue(GridTestUtils.waitForCondition(() ->
                snpMgr.currentSnapshotTask(CreateDumpFutureTask.class) != null, 10_000, 10));

            CreateDumpFutureTask task = snpMgr.currentSnapshotTask(CreateDumpFutureTask.class);

            List<Long> processedVals = new ArrayList<>();

            assertTrue(GridTestUtils.waitForCondition(() -> {
                processedVals.add(task.processedSize());

                return fut.isDone();
            }, getTestTimeout(), 100));

            assertTrue("Expected distinct values: " + processedVals,
                processedVals.stream().mapToLong(v -> v).distinct().count() >= keysCnt);

            assertTrue("Expected sorted values: " + processedVals,
                F.isSorted(processedVals.stream().mapToLong(v -> v).toArray()));
        }
    }

    /** */
    public class TestCacheConflictResolutionManager<K, V> extends GridCacheManagerAdapter<K, V>
        implements CacheConflictResolutionManager<K, V> {

        /** {@inheritDoc} */
        @Override public CacheVersionConflictResolver conflictResolver() {
            return new CacheVersionConflictResolver() {
                @Override public <K, V> GridCacheVersionConflictContext<K, V> resolve(
                    CacheObjectValueContext ctx,
                    GridCacheVersionedEntryEx<K, V> oldEntry,
                    GridCacheVersionedEntryEx<K, V> newEntry,
                    boolean atomicVerComparator
                ) {
                    GridCacheVersionConflictContext res = new GridCacheVersionConflictContext<>(ctx, oldEntry, newEntry);

                    res.useNew();

                    return res;
                }
            };
        }
    }
}
