/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.io.File;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
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
import org.junit.Test;
import org.junit.runners.Parameterized;

import static java.nio.file.Files.newDirectoryStream;
import static org.apache.ignite.configuration.IgniteConfiguration.DFLT_SNAPSHOT_THREAD_POOL_SIZE;
import static org.apache.ignite.testframework.GridTestUtils.cartesianProduct;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.junit.Assume.assumeFalse;

/** Cluster-wide snapshot list procedure tests.*/
public class IgniteClusterSnapshotListTest extends AbstractSnapshotSelfTest {
    /** Number of cache keys to pre-create at node start. */
    private static final int CACHE_KEYS_RANGE = 10;

    /** Number of partitions within a snapshot cache group. */
    private static final int CACHE_PARTITIONS_COUNT = 4;

    /** */
    @Parameterized.Parameter(2)
    public int snpThrdPoolSz;

    /** */
    private PluginProvider<PluginConfiguration> pluginProvider;

    /** Parameters. */
    @Parameterized.Parameters(name = "encryption={0}, onlyPrimary={1}, snpThrdPoolSz={2}")
    public static Collection<Object[]> params() {
        return cartesianProduct(
            encryptionParameters(),
            F.asList(false, true),
            F.asList(DFLT_SNAPSHOT_THREAD_POOL_SIZE, 1)
        );
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName)
            .setWorkDirectory(new File(U.defaultWorkDirectory(), igniteInstanceName).getAbsolutePath());

        if (pluginProvider != null)
            cfg.setPluginProviders(pluginProvider);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected <K, V> CacheConfiguration<K, V> txCacheConfig(CacheConfiguration<K, V> ccfg) {
        // Fastent the tests.
        return super.txCacheConfig(ccfg).setAffinity(new RendezvousAffinityFunction(false, CACHE_PARTITIONS_COUNT));
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

    /** */
    @Test
    public void testConcurrentCreation() throws Exception {
        CountDownLatch proceedSnpCreation = new CountDownLatch(1);
        CountDownLatch beginSnpCreation = new CountDownLatch(1);

        int grids = 3;
        int testNodeIdx = 1;
        int testNodeOrder = testNodeIdx + 1;

        // Delays snapshot creation after its metadata is written.
        pluginProvider = new AbstractTestPluginProvider() {
            @Override public String name() {
                return "TestSnpMgrProvider";
            }

            @Override public <T> T createComponent(PluginContext ctx, Class<T> cls) {
                if (IgniteSnapshotManager.class.isAssignableFrom(cls)) {
                    return (T)new IgniteSnapshotManager(((IgniteEx)ctx.grid()).context()) {
                        @Override public <M extends Serializable> void storeSnapshotMeta(M meta, File smf) {
                            beginSnpCreation.countDown();

                            super.storeSnapshotMeta(meta, smf);

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

        IgniteFuture<Void> createSnpFut = snp(grid(0)).createSnapshot(SNAPSHOT_NAME);

        assertTrue(beginSnpCreation.await(getTestTimeout(), TimeUnit.MILLISECONDS));

        SnapshotListTaskResult lstOpRes = listSnapshots(grid(2));

        int snpsCnt = Stream.of(lstOpRes.snapshots()).mapToInt(jr -> jr.snapshots().size()).sum();

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
        CountDownLatch metaReadProceedLatch = new CountDownLatch(1);
        CountDownLatch metaReadBeginLatch = new CountDownLatch(1);

        pluginProvider = new AbstractTestPluginProvider() {
            @Override public String name() {
                return "TestSnpMgrProvider";
            }

            @Override public <T> T createComponent(PluginContext ctx, Class<T> cls) {
                if (IgniteSnapshotManager.class.isAssignableFrom(cls)) {
                    return (T)new IgniteSnapshotManager(((IgniteEx)ctx.grid()).context()) {
                        @Override public List<SnapshotMetadata> readSnapshotMetadatas(SnapshotFileTree sft,
                            boolean failIfCantRead) {
                            List<SnapshotMetadata> metas = super.readSnapshotMetadatas(sft, failIfCantRead);

                            if (cctx.localNode().order() != 2)
                                return metas;

                            metaReadBeginLatch.countDown();

                            try {
                                assertTrue(metaReadProceedLatch.await(getTestTimeout(), TimeUnit.MILLISECONDS));
                            }
                            catch (InterruptedException e) {
                                throw new IllegalStateException(e);
                            }

                            return metas;
                        }
                    };
                }

                return super.createComponent(ctx, cls);
            }
        };

        startGridsWithSnapshot(3, CACHE_KEYS_RANGE, true, false);

        SnapshotFileTree grid1SnpSft = new SnapshotFileTree(grid(1).context(), SNAPSHOT_NAME, null);

        long testSnpSz = SnapshotListTask.calculateDirectorySize(grid1SnpSft.root());

        assertTrue(testSnpSz > 0);

        IgniteInternalFuture<SnapshotListTaskResult> lstOpFut = runAsync(() -> listSnapshots(grid(0)));

        assertTrue(metaReadBeginLatch.await(getTestTimeout(), TimeUnit.MILLISECONDS));

        if (completeDeletion)
            assertTrue(grid1SnpSft.root().exists() && U.delete(grid1SnpSft.root()) && !grid1SnpSft.root().exists());
        else
            assertTrue(grid1SnpSft.nodeStorage().exists() && U.delete(grid1SnpSft.nodeStorage()) && !grid1SnpSft.nodeStorage().exists());

        metaReadProceedLatch.countDown();

        SnapshotListTaskResult lstOpRes = lstOpFut.get();

        if (completeDeletion) {
            int snpsCnt = Stream.of(lstOpRes.snapshots()).mapToInt(jr -> jr.snapshots().size()).sum();

            assertEquals(2, snpsCnt);
        }
        else {
            int snpsCnt = 0;
            boolean victimNodeFound = false;

            for (int i = 0; i < lstOpRes.nodesIds().length; i++) {
                UUID nid = lstOpRes.nodesIds()[i];

                Map<String, SnapshotListJobResult.SnapshotInfo> nodeSnps = lstOpRes.snapshots()[i].snapshots();

                snpsCnt += nodeSnps.size();

                if (nid.equals(grid(1).localNode().id())) {
                    victimNodeFound = true;

                    assertEquals(1, nodeSnps.size());

                    long curSnpSz = SnapshotListTask.calculateDirectorySize(grid1SnpSft.root());

                    assertTrue(curSnpSz < testSnpSz);
                }
            }

            assertTrue(victimNodeFound);
            assertEquals(3, snpsCnt);
        }
    }

    /**
     * Ensures that a file can be read without locking or other issues while bieng cuncurently written on curren OS and
     * file system. This behavior is important for the case when snapshots is being read while creation.
     */
    @Test
    public void testReadWhileCreating() throws Exception {
        // Doesn't matter here. Fastens the tests.
        assumeFalse(encryption || onlyPrimary || snpThrdPoolSz > 1);

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
        startGridsWithSnapshot(3, CACHE_KEYS_RANGE, true, false);

        SnapshotFileTree sftNode1 = new SnapshotFileTree(grid(1).context(), SNAPSHOT_NAME, null);

        assertTrue(sftNode1.meta().exists());

        if (corruptFile) {
            try (RandomAccessFile raf = new RandomAccessFile(sftNode1.meta(), "rw")) {
                raf.write(UUID.randomUUID().toString().getBytes());
            }
        }
        else
            assertTrue(sftNode1.meta().delete() && !sftNode1.meta().exists());

        SnapshotListTaskResult res = listSnapshots(grid(2));

        int foundSnpsCnt = 0;
        boolean victimNodeFound = false;

        for (int i = 0; i < res.nodesIds().length; i++) {
            UUID nid = res.nodesIds()[i];

            foundSnpsCnt += res.snapshots()[i].snapshots().size();

            if (nid.equals(grid(1).localNode().id())) {
                victimNodeFound = true;

                assertTrue(res.snapshots()[i].snapshots().isEmpty());
            }
        }

        assertTrue(victimNodeFound);
        assertEquals(2, foundSnpsCnt);
    }

    /** */
    private static SnapshotListTaskResult listSnapshots(IgniteEx grid) throws Exception {
        SnapshotListCommandArg arg = new SnapshotListCommandArg();

        Collection<UUID> nodes = grid.cluster().forServers().nodes().stream().map(ClusterNode::id).toList();

        return grid.compute().execute(SnapshotListTask.class, new VisorTaskArgument<>(nodes, arg, false)).result();
    }
}
