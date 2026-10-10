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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.management.snapshot.SnapshotListCommandArg;
import org.apache.ignite.internal.management.snapshot.SnapshotListTask;
import org.apache.ignite.internal.processors.cache.persistence.filename.SnapshotFileTree;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.PluginConfiguration;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.plugin.PluginProvider;
import org.jetbrains.annotations.Nullable;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized;

import static java.nio.file.Files.newDirectoryStream;
import static org.apache.ignite.configuration.IgniteConfiguration.DFLT_SNAPSHOT_THREAD_POOL_SIZE;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;
import static org.apache.ignite.testframework.GridTestUtils.cartesianProduct;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

/** Cluster-wide snapshot list procedure tests. */
public class IgniteClusterSnapshotListTest extends AbstractSnapshotSelfTest {
    /** Number of cache keys to pre-create at node start. */
    private static final int CACHE_KEYS_RANGE = 15;

    /** Number of partitions within a snapshot cache group. */
    private static final int CACHE_PARTITIONS_COUNT = 4;

    /** */
    private static boolean posixPermissions;

    /** Size of the snapshot utility thread pool. */
    @Parameterized.Parameter(2)
    public int snpThrdPoolSz;

    /** */
    private PluginProvider<PluginConfiguration> pluginProvider;

    /** Flag to spread the test cache data over the external storages. */
    private boolean extStorages;

    /** Resolved external storages paths. {@code null} if {@link #extStorages} is {@code false}. */
    private @Nullable String[] extStoragePaths;

    /** Parameters. */
    @Parameterized.Parameters(name = "encryption={0}, onlyPrimary={1}, snpThrdPoolSz={2}")
    public static Collection<Object[]> params() {
        return cartesianProduct(
            encryptionParameters(), // Encryption
            F.asList(false, true), // Only primary
            F.asList(DFLT_SNAPSHOT_THREAD_POOL_SIZE, 1) // Snapshots thread pool size
        );
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName)
            .setWorkDirectory(new File(U.defaultWorkDirectory(), igniteInstanceName).getAbsolutePath());

        if (pluginProvider != null)
            cfg.setPluginProviders(pluginProvider);

        if (extStorages) {
            // External storage paths must be identical on all the nodes (a cache has the single storage paths
            // setting). Thus, a shared work directory is used, like in GridCommandHandlerListSnapshotTest.
            cfg.setWorkDirectory(U.defaultWorkDirectory());

            cfg.getDataStorageConfiguration().setExtraStoragePaths(
                U.defaultWorkDirectory() + File.separator,
                U.defaultWorkDirectory() + File.separator + "extStorage"
            );

            extStoragePaths = cfg.getDataStorageConfiguration().getExtraStoragePaths();

            cfg.getDataStorageConfiguration().setExtraSnapshotPaths("", "extStorage");
        }

        cfg.setSnapshotThreadPoolSize(snpThrdPoolSz);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void cleanPersistenceDir() throws Exception {
        super.cleanPersistenceDir();

        // Also cleans separated snapshot working directories and custom snapshot paths.
        try (DirectoryStream<Path> files = newDirectoryStream(Paths.get(U.defaultWorkDirectory()))) {
            for (Path path : files)
                U.delete(path);
        }
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        File workDir = new File(U.defaultWorkDirectory());

        assertTrue(workDir.exists());

        try {
            Files.getPosixFilePermissions(workDir.toPath());

            posixPermissions = true;
        }
        catch (UnsupportedOperationException ignored) {
            // No-op.
        }
    }

    /** {@inheritDoc} */
    @Override protected <K, V> CacheConfiguration<K, V> txCacheConfig(CacheConfiguration<K, V> ccfg) {
        // Speeds up the tests.
        ccfg = super.txCacheConfig(ccfg)
            .setAffinity(new RendezvousAffinityFunction(false, CACHE_PARTITIONS_COUNT))
            .setBackups(1);

        if (extStorages) {
            assert !F.isEmpty(extStoragePaths);

            ccfg.setStoragePaths(extStoragePaths);
        }

        return ccfg;
    }

    /** */
    @Test
    public void testConcurrentCreation() throws Exception {
        // The test uses the thread blocking and conditional waitings. Won't proceed with 1 thread.
        assumeTrue(snpThrdPoolSz > 1);

        int grids = 3;
        int testNodeIdx = 1;
        int testNodeOrder = testNodeIdx + 1;

        CountDownLatch beginSnpCreation = new CountDownLatch(grids);
        CountDownLatch proceedSnpCreation = new CountDownLatch(1);

        // Delays snapshot creation after its metadata is written.
        pluginProvider = new AbstractTestPluginProvider() {
            @Override public String name() {
                return "TestSnpMgrProvider";
            }

            @Override public <T> T createComponent(PluginContext ctx, Class<T> cls) {
                if (IgniteSnapshotManager.class.isAssignableFrom(cls)) {
                    return (T)new IgniteSnapshotManager(((IgniteEx)ctx.grid()).context()) {
                        @Override public <M extends Serializable> void storeSnapshotMeta(M meta, File smf) {
                            super.storeSnapshotMeta(meta, smf);

                            beginSnpCreation.countDown();

                            if (((IgniteEx)ctx.grid()).localNode().order() == testNodeOrder) {
                                try {
                                    assertTrue(proceedSnpCreation.await(getTestTimeout(), TimeUnit.MILLISECONDS));
                                }
                                catch (InterruptedException e) {
                                    throw new IllegalStateException(e);
                                }
                            }
                        }
                    };
                }

                return super.createComponent(ctx, cls);
            }
        };

        startGridsWithCache(grids, txCacheConfig(defaultCacheConfiguration()), CACHE_KEYS_RANGE);

        IgniteFuture<Void> createSnpFut = snp(grid(0)).createSnapshot(SNAPSHOT_NAME, null, false, onlyPrimary);

        assertTrue(beginSnpCreation.await(getTestTimeout(), TimeUnit.MILLISECONDS));

        SnapshotListTaskResult lstOpRes = listSnapshots(grid(2));

        int snpsCnt = Stream.of(lstOpRes.nodesSnapshots()).mapToInt(jr -> jr.snapshots().size()).sum();

        assertEquals(grids, snpsCnt);

        SnapshotFileTree testSft = new SnapshotFileTree(grid(testNodeIdx).context(), SNAPSHOT_NAME, null);

        assertTrue(SnapshotListTask.calculateDirectorySize(testSft.root()) > 0L);

        proceedSnpCreation.countDown();

        createSnpFut.get(getTestTimeout());
    }

    /** */
    @Test
    public void testCompletelyDeletedAfterMetaRead() throws Exception {
        doTestDeletionAfterMetaRead(true);
    }

    /** */
    @Test
    public void testPartlyDeletedAfterMetaRead() throws Exception {
        doTestDeletionAfterMetaRead(false);
    }

    /** */
    private void doTestDeletionAfterMetaRead(boolean completeDeletion) throws Exception {
        int grids = 3;
        int testGridIdx = 1;

        CountDownLatch metaReadBeginLatch = new CountDownLatch(grids);
        CountDownLatch metaReadProceedLatch = new CountDownLatch(1);

        pluginProvider = new AbstractTestPluginProvider() {
            @Override public String name() {
                return "TestSnpMgrProvider";
            }

            @Override public <T> T createComponent(PluginContext ctx, Class<T> cls) {
                if (IgniteSnapshotManager.class.isAssignableFrom(cls)) {
                    return (T)new IgniteSnapshotManager(((IgniteEx)ctx.grid()).context()) {
                        @Override public List<SnapshotMetadata> readSnapshotMetadatas(SnapshotFileTree sft, boolean failIfCantRead) {
                            metaReadBeginLatch.countDown();

                            if (cctx.localNode().order() == testGridIdx + 1) {
                                try {
                                    assertTrue(metaReadProceedLatch.await(getTestTimeout(), TimeUnit.MILLISECONDS));
                                }
                                catch (InterruptedException e) {
                                    throw new IllegalStateException(e);
                                }
                            }

                            return super.readSnapshotMetadatas(sft, failIfCantRead);
                        }
                    };
                }

                return super.createComponent(ctx, cls);
            }
        };

        startGridsWithSnapshot(grids, CACHE_KEYS_RANGE, true, false);

        SnapshotFileTree testSnpFt = new SnapshotFileTree(grid(testGridIdx).context(), SNAPSHOT_NAME, null);

        long testSnpSz = SnapshotListTask.calculateDirectorySize(testSnpFt.root());

        assertTrue(testSnpSz > 0);

        IgniteInternalFuture<SnapshotListTaskResult> lstOpFut = runAsync(() -> listSnapshots(grid(0)));

        assertTrue(metaReadBeginLatch.await(getTestTimeout(), TimeUnit.MILLISECONDS));

        if (completeDeletion)
            assertTrue(testSnpFt.root().exists() && U.delete(testSnpFt.root()) && !testSnpFt.root().exists());
        else
            assertTrue(testSnpFt.nodeStorage().exists() && U.delete(testSnpFt.nodeStorage()) && !testSnpFt.nodeStorage().exists());

        metaReadProceedLatch.countDown();

        SnapshotListTaskResult lstOpRes = lstOpFut.get();

        if (completeDeletion) {
            int snpsCnt = Stream.of(lstOpRes.nodesSnapshots()).mapToInt(jr -> jr.snapshots().size()).sum();

            assertEquals(grids - 1, snpsCnt);
        }
        else {
            int snpsCnt = 0;
            boolean victimNodeFound = false;

            for (int i = 0; i < lstOpRes.nodesIds().length; i++) {
                UUID nid = lstOpRes.nodesIds()[i];

                Map<String, SnapshotListJobResult.SnapshotInfo> nodeSnps = lstOpRes.nodesSnapshots()[i].snapshots();

                snpsCnt += nodeSnps.size();

                if (nid.equals(grid(testGridIdx).localNode().id())) {
                    victimNodeFound = true;

                    assertEquals(1, nodeSnps.size());

                    long curSnpSz = SnapshotListTask.calculateDirectorySize(testSnpFt.root());

                    assertTrue(curSnpSz < testSnpSz);
                }
            }

            assertTrue(victimNodeFound);
            assertEquals(grids, snpsCnt);
        }
    }

    /**
     * Ensures that a file can be read without locking or other issues while being concurrently written on current OS and
     * file system. This behavior is important for the case when snapshots are being read while creation.
     */
    @Test
    public void testReadWhileCreating() throws Exception {
        // Doesn't matter here. speeds up the tests.
        assumeFalse(encryption || onlyPrimary);

        assertTrue(new File(U.defaultWorkDirectory()).exists());

        File testF = new File(U.defaultWorkDirectory(), "test.out");

        assumeFalse(testF.exists());

        CountDownLatch proceedWriteLatch = new CountDownLatch(1);
        AtomicBoolean readFlag = new AtomicBoolean();

        try {
            try (RandomAccessFile raf = new RandomAccessFile(testF, "rw");) {
                raf.write(1);
                raf.write(2);

                Thread t = new Thread(() -> {
                    try (RandomAccessFile raf0 = new RandomAccessFile(testF, "r")) {
                        assertEquals(1, raf0.read());
                        assertEquals(2, raf0.read());

                        readFlag.set(true);

                        proceedWriteLatch.countDown();
                    }
                    catch (Throwable e) {
                        log.error("Failed to concurrently read the test file.", e);
                    }
                });

                t.setDaemon(true);
                t.start();

                assertTrue(proceedWriteLatch.await(getTestTimeout(), TimeUnit.MILLISECONDS));

                raf.write(3);
            }

            try (RandomAccessFile raf = new RandomAccessFile(testF, "r");) {
                assertEquals(1, raf.read());
                assertEquals(2, raf.read());
                assertEquals(3, raf.read());
            }
        }
        finally {
            assertTrue(!testF.exists() || testF.delete());
        }

        assertTrue(readFlag.get());
    }

    /** */
    @Test
    public void testMissingSnapshotPath() throws Exception {
        doTestWrongSnapshotPath(true);
    }

    /** */
    @Test
    public void testEmptySnapshotPath() throws Exception {
        doTestWrongSnapshotPath(false);
    }

    /** */
    private void doTestWrongSnapshotPath(boolean missing) throws Exception {
        // Doesn't matter here, speeds up the tests.
        assumeFalse(encryption || onlyPrimary || snpThrdPoolSz < 2);

        doTestSnapshotPath(null);

        File path = new File(U.defaultWorkDirectory(), "not_snapshots");

        if (!missing)
            assertTrue(new File(path, SNAPSHOT_NAME).mkdirs());

        SnapshotListJobResult[] lstOpRes = listSnapshots(grid(0), path.getAbsolutePath()).nodesSnapshots();

        int cnt = Stream.of(lstOpRes).mapToInt(nodeRes -> nodeRes.snapshots().size()).sum();

        assertEquals(0, cnt);
    }

    /** */
    @Test
    public void testDefaultSnapshotPath() throws Exception {
        doTestSnapshotPath(null);
    }

    /** */
    @Test
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-29126")
    public void testRelativeSnapshotPath() throws Exception {
        doTestSnapshotPath("ex_snapshots");
    }

    /** */
    @Test
    public void testAbsoluteSnapshotPath() throws Exception {
        doTestSnapshotPath(new File(U.defaultWorkDirectory(), "ex_snapshots").getAbsolutePath());
    }

    /** */
    private void doTestSnapshotPath(@Nullable String path) throws Exception {
        int grids = 3;

        startGridsWithCache(grids, txCacheConfig(defaultCacheConfiguration()), CACHE_KEYS_RANGE);

        snp(grid(0)).createSnapshot(SNAPSHOT_NAME, path, false, onlyPrimary).get(getTestTimeout());

        SnapshotListJobResult[] lstOpRes = listSnapshots(grid(0), path).nodesSnapshots();

        int cnt = Stream.of(lstOpRes).mapToInt(nodeRes -> nodeRes.snapshots().size()).sum();

        assertEquals(grids, cnt);
    }

    /** */
    @Test
    public void testMissingMeta() throws Exception {
        doTestWithWrongMeta(false);
    }

    /** */
    @Test
    public void testCorruptedMeta() throws Exception {
        doTestWithWrongMeta(true);
    }

    /**
     * Tests snapshot lists when one snapshot metadata cannot be read.
     *
     * @param corruptFile If {@code true}, corrupts metadata. Otherwise, deletes metadata.
     */
    private void doTestWithWrongMeta(boolean corruptFile) throws Exception {
        // Speeds up the tests.
        assumeFalse(encryption);

        int grids = 3;
        int testGridIdx = 1;

        startGridsWithSnapshot(grids, CACHE_KEYS_RANGE, true, false);

        SnapshotFileTree testSnpFt = new SnapshotFileTree(grid(testGridIdx).context(), SNAPSHOT_NAME, null);

        assertTrue(testSnpFt.meta().exists());

        if (corruptFile) {
            try (RandomAccessFile raf = new RandomAccessFile(testSnpFt.meta(), "rw")) {
                raf.write(UUID.randomUUID().toString().getBytes());
            }
        }
        else
            assertTrue(testSnpFt.meta().delete() && !testSnpFt.meta().exists());

        SnapshotListTaskResult res = listSnapshots(grid(0));

        int foundSnpsCnt = 0;
        boolean victimNodeFound = false;

        for (int i = 0; i < res.nodesIds().length; i++) {
            UUID nid = res.nodesIds()[i];

            foundSnpsCnt += res.nodesSnapshots()[i].snapshots().size();

            if (nid.equals(grid(testGridIdx).localNode().id())) {
                victimNodeFound = true;

                assertTrue(res.nodesSnapshots()[i].snapshots().isEmpty());
            }
        }

        assertTrue(victimNodeFound);
        assertEquals(grids - 1, foundSnpsCnt);
    }

    /** */
    @Test
    public void testMissingIncrementalMeta() throws Exception {
        doTestWithWrongIncrementalMeta(false);
    }

    /** */
    @Test
    public void testCorruptedIncrementalMeta() throws Exception {
        doTestWithWrongIncrementalMeta(true);
    }

    /**
     * Tests snapshot list when incremental snapshot metadata cannot be read.
     * The main snapshot should still be listed, but without incremental info on the affected node.
     *
     * @param corruptFile If {@code true}, corrupts metadata. Otherwise, deletes metadata.
     */
    private void doTestWithWrongIncrementalMeta(boolean corruptFile) throws Exception {
        // Incremental snapshots do not support the only-primary mode or encryption.
        assumeFalse(onlyPrimary || encryption);

        int grids = 3;
        int testGridIdx = 1;
        int incsCnt = 3;

        IgniteEx ig = startGridsWithCache(grids, txCacheConfig(defaultCacheConfiguration()), CACHE_KEYS_RANGE);

        snp(ig).createSnapshot(SNAPSHOT_NAME, null, false, onlyPrimary).get(getTestTimeout());

        for (int i = 0; i < incsCnt; ++i) {
            try (IgniteDataStreamer<Integer, Integer> ds = grid(0).dataStreamer(DEFAULT_CACHE_NAME)) {
                for (int kv = (i + 1) * CACHE_KEYS_RANGE; kv < (i + 1) * CACHE_KEYS_RANGE * 2; ++kv)
                    ds.addData(kv, kv);
            }

            snp(ig).createSnapshot(SNAPSHOT_NAME, null, true, onlyPrimary).get(getTestTimeout());
        }

        SnapshotFileTree testSnpFt = new SnapshotFileTree(grid(testGridIdx).context(), SNAPSHOT_NAME, null);
        SnapshotFileTree.IncrementalSnapshotFileTree incFt = testSnpFt.incrementalSnapshotFileTree(1);
        File incMeta = incFt.meta();

        assertTrue(incMeta.exists());

        if (corruptFile) {
            try (RandomAccessFile raf = new RandomAccessFile(incMeta, "rw")) {
                raf.write(UUID.randomUUID().toString().getBytes());
            }
        }
        else
            assertTrue(incMeta.delete() && !incMeta.exists());

        SnapshotListTaskResult res = listSnapshots(grid(0));

        for (int i = 0; i < res.nodesIds().length; i++) {
            UUID nid = res.nodesIds()[i];

            Map<String, SnapshotListJobResult.SnapshotInfo> snps = res.nodesSnapshots()[i].snapshots();

            assertEquals(1, snps.size());

            SnapshotListJobResult.SnapshotInfo info = snps.get(SNAPSHOT_NAME);

            assertNotNull(info);
            assertNotNull(info.incrementals());

            if (nid.equals(grid(testGridIdx).localNode().id()))
                assertEquals(incsCnt - 1, info.incrementals().number().intValue());
            else
                assertEquals(incsCnt, info.incrementals().number().intValue());
        }
    }

    /**
     * Test snapshot list operation when a node can't read some snapshot part due to insufficient permissions.
     * I.e. a test node is able to read snapshot meta but can't read some of the snapshot's data.
     */
    @Test
    public void testDeniedPermissions() throws Exception {
        assumeTrue(posixPermissions);
        // We rely on sizes here. Better to avoid empty data nodes not to become flaky.
        assumeFalse(onlyPrimary);

        int grids = 3;
        int testGridIdx = 1;

        // Permissions to restore.
        Map<Path, Set<PosixFilePermission>> oldPerms = new ConcurrentHashMap<>();
        // The 'change permissions' flag.
        AtomicBoolean changePermissions = new AtomicBoolean(true);

        // Deny reading on a couple of incremental snapshot metadata files on one node.
        pluginProvider = new AbstractTestPluginProvider() {
            @Override public String name() {
                return "TestSnpMgrProvider";
            }

            @Override public <T> T createComponent(PluginContext ctx, Class<T> cls) {
                if (IgniteSnapshotManager.class.isAssignableFrom(cls)) {
                    return (T)new IgniteSnapshotManager(((IgniteEx)ctx.grid()).context()) {
                        @Override public List<SnapshotMetadata> readSnapshotMetadatas(SnapshotFileTree sft,
                            boolean failIfCantRead) {
                            if (changePermissions.get() && ctx.localNode().order() == testGridIdx + 1) {
                                File victimDir = sft.nodeStorage();

                                assertTrue(victimDir.exists());
                                assertTrue(victimDir.isDirectory());

                                // Ensure that blocked snapshot part has some size. We use sizes to compare later.
                                try {
                                    assertTrue(SnapshotListTask.calculateDirectorySize(victimDir) > 0L);
                                }
                                catch (IOException e) {
                                    throw new IllegalStateException(e);
                                }

                                Path victimDirPath = victimDir.toPath();

                                try {
                                    Set<PosixFilePermission> perms = Files.getPosixFilePermissions(victimDirPath);

                                    assertFalse(perms.isEmpty());

                                    // Deny reading.
                                    Files.setPosixFilePermissions(victimDirPath, PosixFilePermissions.fromString("---------"));

                                    // Save actual permissions to restore.
                                    oldPerms.put(victimDirPath, perms);
                                }
                                catch (Exception e) {
                                    throw new IgniteException("Unable to set the test posix permissions.", e);
                                }
                            }

                            return super.readSnapshotMetadatas(sft, failIfCantRead);
                        }
                    };
                }

                return super.createComponent(ctx, cls);
            }
        };

        startGridsWithSnapshot(grids, CACHE_KEYS_RANGE, true, true);

        UUID testNodeId = grid(testGridIdx).localNode().id();

        // Snapshot size with restricted permissions.
        long testSize0 = 0L;
        // Snapshot size with normal permissions.
        long testSize1 = 0L;

        SnapshotListTaskResult snpLstOpRes;

        try {
            // First run.
            snpLstOpRes = listSnapshots(grid(0));

            assertFalse(oldPerms.isEmpty());

            for (int i = 0; i < snpLstOpRes.nodesIds().length; i++) {
                UUID nodeId = snpLstOpRes.nodesIds()[i];

                // Store size of the partly read snapshot.
                if (nodeId.equals(testNodeId)) {
                    Map<String, SnapshotListJobResult.SnapshotInfo> snps = snpLstOpRes.nodesSnapshots()[i].snapshots();

                    assertEquals(1, snps.size());

                    testSize0 = snps.get(SNAPSHOT_NAME).size();
                }
            }

            // Ensure that we've found and read snapshot on the test node.
            assertTrue(testSize0 > 0L);
        }
        finally {
            // Restore the permissions in any case.
            oldPerms.forEach((path, perms) -> {
                try {
                    Files.setPosixFilePermissions(path, perms);
                }
                catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            });
        }

        // Relaunch the operation.
        changePermissions.set(false);

        snpLstOpRes = listSnapshots(grid(0));

        for (int i = 0; i < snpLstOpRes.nodesIds().length; i++) {
            UUID nodeId = snpLstOpRes.nodesIds()[i];

            // Store size of the partly read snapshot.
            if (nodeId.equals(testNodeId)) {
                Map<String, SnapshotListJobResult.SnapshotInfo> snps = snpLstOpRes.nodesSnapshots()[i].snapshots();

                assertEquals(1, snps.size());

                testSize1 = snps.get(SNAPSHOT_NAME).size();
            }
        }

        // Ensure that the calculated anew size is bigger than in the previous run.
        assertTrue(testSize1 > 0L);
        assertTrue(testSize1 > testSize0);
    }

    /** */
    @Test
    public void testSnapshotListsDates() throws Exception {
        // Incremental snapshots do not support the only-primary mode or encryption.
        assumeFalse(onlyPrimary || encryption);

        int grids = 3;

        IgniteEx ig = startGridsWithCache(grids, txCacheConfig(defaultCacheConfiguration()), CACHE_KEYS_RANGE);

        long time0 = U.currentTimeMillis();

        // Wait for a while, spend some time.
        U.sleep(300L);

        snp(ig).createSnapshot(SNAPSHOT_NAME, null, false, onlyPrimary).get(getTestTimeout());

        SnapshotListTaskResult res = listSnapshots(grid(0));

        for (int i = 0; i < res.nodesIds().length; i++) {
            Map<String, SnapshotListJobResult.SnapshotInfo> snps = res.nodesSnapshots()[i].snapshots();

            assertEquals(1, snps.size());

            SnapshotListJobResult.SnapshotInfo info = snps.get(SNAPSHOT_NAME);

            assertTrue(info.date() > time0);
        }

        // Wait for a while, spend some time.
        U.sleep(300L);

        long time1 = U.currentTimeMillis();

        try (IgniteDataStreamer<Integer, Integer> ds = ig.dataStreamer(DEFAULT_CACHE_NAME)) {
            for (int kv = CACHE_KEYS_RANGE; kv < CACHE_KEYS_RANGE * 2; kv++)
                ds.addData(kv, kv);
        }

        snp(ig).createSnapshot(SNAPSHOT_NAME, null, true, onlyPrimary).get(getTestTimeout());

        SnapshotListTaskResult res2 = listSnapshots(grid(0));

        for (int i = 0; i < res2.nodesIds().length; i++) {
            Map<String, SnapshotListJobResult.SnapshotInfo> snps = res2.nodesSnapshots()[i].snapshots();

            assertEquals(1, snps.size());

            SnapshotListJobResult.SnapshotInfo info = snps.get(SNAPSHOT_NAME);

            assertNotNull(info.incrementals());

            assertTrue(info.date() < time1);
            assertTrue(info.date() < info.incrementals().date());
            assertTrue(info.incrementals().date() > time1);
        }
    }

    /** */
    @Test
    public void testSnapshotListsSizes() throws Exception {
        // Incremental snapshots do not support the only-primary mode or encryption.
        assumeFalse(onlyPrimary || encryption);
        // Speeds up the tests.
        assumeTrue(snpThrdPoolSz > 1);

        int grids = 3;

        IgniteEx ig = startGridsWithCache(grids, txCacheConfig(defaultCacheConfiguration()), CACHE_KEYS_RANGE);

        snp(ig).createSnapshot(SNAPSHOT_NAME, null, false, onlyPrimary).get(getTestTimeout());

        SnapshotListTaskResult res = listSnapshots(grid(0));

        Map<UUID, Long> sizes0 = collectSizes(res);

        // Check the sizes.
        for (int g = 0; g < grids; g++) {
            long sz = SnapshotListTask.calculateDirectorySize(new SnapshotFileTree(grid(g).context(), SNAPSHOT_NAME, null).root());

            assertEquals(sz, sizes0.get(grid(g).localNode().id()).longValue());

            assertNull(snapshotInfo(res, grid(g)).externalStorages());
            assertNull(snapshotInfo(res, grid(g)).incrementals());
        }

        // Add some data and create an incremental snapshot.
        try (IgniteDataStreamer<Integer, Integer> ds = ig.dataStreamer(DEFAULT_CACHE_NAME)) {
            for (int kv = CACHE_KEYS_RANGE; kv < CACHE_KEYS_RANGE * 2; ++kv)
                ds.addData(kv, kv);
        }

        snp(ig).createSnapshot(SNAPSHOT_NAME, null, true, onlyPrimary).get(getTestTimeout());

        // Repeat the operation.
        res = listSnapshots(grid(0));

        Map<UUID, Long> sizes1 = collectSizes(res);

        for (int g = 0; g < grids; g++) {
            UUID nodeId = grid(g).localNode().id();

            SnapshotListJobResult.SnapshotInfo info = snapshotInfo(res, grid(g));

            assertNotNull(info.incrementals());
            assertEquals(1, info.incrementals().number().intValue());

            // The size must grow, but exactly to the actual directory size: the incremental part
            // is placed inside the snapshot root and must not be counted twice.
            assertTrue(sizes1.get(nodeId) > sizes0.get(nodeId));

            long sz = SnapshotListTask.calculateDirectorySize(new SnapshotFileTree(grid(g).context(), SNAPSHOT_NAME, null).root());

            assertEquals(sz, sizes1.get(nodeId).longValue());
        }
    }

    /** */
    @Test
    public void testSnapshotListsSizesExternalStorages() throws Exception {
        // Speeds up the tests.
        assumeTrue(snpThrdPoolSz > 1);
        assumeTrue(onlyPrimary);

        int grids = 3;

        extStorages = true;

        // Properly delays the test cache creation with the configured external storages.
        dfltCacheCfg = null;

        startGridsMultiThreaded(grids);

        grid(0).createCache(txCacheConfig(defaultCacheConfiguration()));

        try (IgniteDataStreamer<Integer, Integer> ds = grid(0).dataStreamer(DEFAULT_CACHE_NAME)) {
            for (int i = 0; i < CACHE_KEYS_RANGE; i++)
                ds.addData(i, i);
        }

        snp(grid(0)).createSnapshot(SNAPSHOT_NAME, null, false, onlyPrimary).get(getTestTimeout());

        SnapshotListTaskResult res = listSnapshots(grid(0));

        for (int g = 0; g < grids; g++) {
            SnapshotListJobResult.SnapshotInfo info = snapshotInfo(res, grid(g));

            assertNotNull(info);
            assertNotNull(info.externalStorages());

            long rootSize = SnapshotListTask.calculateDirectorySize(new SnapshotFileTree(grid(g).context(), SNAPSHOT_NAME, null).root());

            // If there is a data withing the snapshot's external storage, its size must be added to the total size.
            assertTrue(info.externalStorages().size() > 0L ? info.size() > rootSize : info.size() == rootSize);
            assertEquals(rootSize + info.externalStorages().size(), info.size());
        }
    }

    /** */
    @Test
    public void testNodeStopDuringSnapshotList() throws Exception {
        // Doesn't matter here, speeds up the tests.
        assumeFalse(encryption || onlyPrimary);

        int grids = 3;
        int testGridIdx = 1;

        CountDownLatch snpLstBeginLatch = new CountDownLatch(grids);
        CountDownLatch snpLstProceedLatch = new CountDownLatch(1);

        // Delays snapshot reading.
        pluginProvider = new AbstractTestPluginProvider() {
            @Override public String name() {
                return "TestSnpMgrProvider";
            }

            @Override public <T> T createComponent(PluginContext ctx, Class<T> cls) {
                if (IgniteSnapshotManager.class.isAssignableFrom(cls)) {
                    return (T)new IgniteSnapshotManager(((IgniteEx)ctx.grid()).context()) {
                        @Override public List<SnapshotMetadata> readSnapshotMetadatas(SnapshotFileTree sft, boolean failIfCantRead) {
                            snpLstBeginLatch.countDown();

                            if (((IgniteEx)ctx.grid()).localNode().order() == testGridIdx + 1) {
                                try {
                                    assertTrue(snpLstProceedLatch.await(getTestTimeout(), TimeUnit.MILLISECONDS));
                                }
                                catch (InterruptedException e) {
                                    throw new IllegalStateException(e);
                                }
                            }

                            return super.readSnapshotMetadatas(sft, failIfCantRead);
                        }
                    };
                }

                return super.createComponent(ctx, cls);
            }
        };

        startGridsWithSnapshot(grids, CACHE_KEYS_RANGE, true);

        IgniteInternalFuture<SnapshotListTaskResult> lstOpFut = runAsync(() -> listSnapshots(grid(0)));

        assertTrue(snpLstBeginLatch.await(getTestTimeout(), TimeUnit.MILLISECONDS));

        IgniteInternalFuture<?> stopFut = runAsync(() -> stopGrid(testGridIdx));

        assertTrue(waitForCondition(() -> grid(testGridIdx).context().isStopping(), getTestTimeout()));

        snpLstProceedLatch.countDown();

        assertThrowsAnyCause(
            null,
            () -> lstOpFut.get(getTestTimeout()),
            NodeStoppingException.class,
            "Node is stopping"
        );

        stopFut.get(getTestTimeout());
    }

    /** */
    private static SnapshotListTaskResult listSnapshots(IgniteEx grid, @Nullable String src) throws Exception {
        SnapshotListCommandArg arg = new SnapshotListCommandArg();

        arg.src(src);

        Collection<UUID> nodes = grid.cluster().forServers().nodes().stream().map(ClusterNode::id).toList();

        return grid.compute().execute(SnapshotListTask.class, new VisorTaskArgument<>(nodes, arg, false)).result();
    }

    /** */
    private static SnapshotListTaskResult listSnapshots(IgniteEx grid) throws Exception {
        return listSnapshots(grid, null);
    }

    /** @return The test snapshot info reported for the node. */
    private static @Nullable SnapshotListJobResult.SnapshotInfo snapshotInfo(SnapshotListTaskResult res, IgniteEx node) {
        for (int i = 0; i < res.nodesIds().length; i++) {
            if (res.nodesIds()[i].equals(node.localNode().id()))
                return res.nodesSnapshots()[i].snapshots().get(SNAPSHOT_NAME);
        }

        return null;
    }

    /** @return The test snapshot sizes reported by the list operation per node id. */
    private static Map<UUID, Long> collectSizes(SnapshotListTaskResult res) {
        Map<UUID, Long> sizes = new HashMap<>();

        for (int i = 0; i < res.nodesIds().length; i++) {
            SnapshotListJobResult.SnapshotInfo info = res.nodesSnapshots()[i].snapshots().get(SNAPSHOT_NAME);

            assertNotNull(info);

            sizes.put(res.nodesIds()[i], info.size());
        }

        return sizes;
    }
}
