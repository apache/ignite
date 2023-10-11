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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionManager;
import org.apache.ignite.internal.processors.cacheobject.UserCacheObjectImpl;
import org.apache.ignite.internal.processors.dr.GridDrType;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.platform.model.User;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.apache.ignite.configuration.IgniteConfiguration.DFLT_SNAPSHOT_DIRECTORY;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_DATA_FILENAME;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_DIR_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.PART_FILE_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.filename.PdsFolderResolver.DB_DEFAULT_FOLDER;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.DFLT_SNAPSHOT_TMP_DIR;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.DUMP_LOCK;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.dump.AbstractCacheDumpTest.DMP_NAME;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.dump.AbstractCacheDumpTest.KEYS_CNT;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.dump.AbstractCacheDumpTest.USER_FACTORY;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.dump.AbstractCacheDumpTest.dump;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.dump.AbstractCacheDumpTest.invokeCheckCommand;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.dump.CreateDumpFutureTask.DUMP_FILE_EXT;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.dump.DumpEntrySerializer.HEADER_SZ;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/** */
public class IgniteCacheDumpSelf2Test extends GridCommonAbstractTest {
    /** */
    private LogListener lsnr;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (lsnr != null) {
            ListeningTestLogger testLog = new ListeningTestLogger(log);

            testLog.registerListener(lsnr);

            cfg.setGridLogger(testLog);
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
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

        assertContains(
            null,
            invokeCheckCommand(ign, DMP_NAME),
            "Conflict partition: PartitionKeyV2 [grpId=" + CU.cacheId(DEFAULT_CACHE_NAME) +
                ", grpName=" + DEFAULT_CACHE_NAME +
                ", partId=" + corruptedPart + "]"
        );
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
                true
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
}
