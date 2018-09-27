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
package org.apache.ignite.internal.processors.cache.persistence;

import java.io.File;
import java.io.IOException;
import java.nio.file.OpenOption;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIODecorator;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;

/**
 * Test class to check that partition files after eviction are destroyed correctly on next checkpoint or crash recovery.
 */
public class IgnitePdsPartitionFilesDestroyTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE = "cache";

    /** Partitions count. */
    private static final int PARTS_CNT = 32;

    /** Set if I/O exception should be thrown on partition file truncation. */
    private boolean failFileIo;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setWalMode(WALMode.LOG_ONLY)
            .setCheckpointFrequency(10 * 60 * 1000)
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setMaxSize(512 * 1024 * 1024)
                    .setPersistenceEnabled(true)
            );

        if (failFileIo)
            dsCfg.setFileIOFactory(new FailingFileIOFactory(new RandomAccessFileIOFactory()));

        cfg.setDataStorageConfiguration(dsCfg);

        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(CACHE)
            .setBackups(1)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setAffinity(new RendezvousAffinityFunction(false, PARTS_CNT));

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        failFileIo = false;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected FailureHandler getFailureHandler(String igniteInstanceName) {
        return new StopNodeFailureHandler();
    }

    /**
     * @param ignite Ignite.
     * @param keysCnt Keys count.
     */
    private void loadData(IgniteEx ignite, int keysCnt, int multiplier) {
        log.info("Load data: keys=" + keysCnt);

        try (IgniteDataStreamer streamer = ignite.dataStreamer(CACHE)) {
            streamer.allowOverwrite(true);

            for (int k = 0; k < keysCnt; k++)
                streamer.addData(k, k * multiplier);
        }
    }

    /**
     * @param ignite Ignite.
     * @param keysCnt Keys count.
     */
    private void checkData(IgniteEx ignite, int keysCnt, int multiplier) {
        log.info("Check data: " + ignite.name() + ", keys=" + keysCnt);

        IgniteCache<Integer, Integer> cache = ignite.cache(CACHE);

        for (int k = 0; k < keysCnt; k++)
            Assert.assertEquals("node = " + ignite.name() + ", key = " + k, (Integer) (k * multiplier), cache.get(k));
    }

    /**
     * Test that partition files have been deleted correctly on next checkpoint.
     *
     * @throws Exception If failed.
     */
    public void testPartitionFileDestroyAfterCheckpoint() throws Exception {
        IgniteEx crd = (IgniteEx) startGrids(2);

        crd.cluster().active(true);

        int keysCnt = 50_000;

        loadData(crd, keysCnt, 1);

        startGridsMultiThreaded(2, 2);

        // Trigger partitions eviction.
        resetBaselineTopology();

        awaitPartitionMapExchange(true, true, null);

        checkPartitionFiles(crd, true);

        // This checkpoint should delete partition files.
        forceCheckpoint();

        checkPartitionFiles(crd, false);

        for (Ignite ignite : G.allGrids())
            checkData((IgniteEx) ignite, keysCnt, 1);
    }

    /**
     * Test that partition files are reused correctly.
     *
     * @throws Exception If failed.
     */
    public void testPartitionFileDestroyAndRecreate() throws Exception {
        IgniteEx crd = startGrid(0);

        IgniteEx node = startGrid(1);

        crd.cluster().active(true);

        int keysCnt = 50_000;

        loadData(crd, keysCnt, 1);

        startGridsMultiThreaded(2, 2);

        // Trigger partitions eviction.
        resetBaselineTopology();

        awaitPartitionMapExchange(true, true, null);

        checkPartitionFiles(node, true);

        // Trigger partitions re-create.
        stopGrid(2);

        resetBaselineTopology();

        awaitPartitionMapExchange(true, true, null);

        checkPartitionFiles(node, true);

        // Rewrite data.
        loadData(crd, keysCnt, 2);

        // Force checkpoint on all nodes.
        forceCheckpoint();

        // Check that all unecessary partition files have been deleted.
        checkPartitionFiles(node, false);

        for (Ignite ignite : G.allGrids())
            checkData((IgniteEx) ignite, keysCnt, 2);
    }

    /**
     * Test that partitions files have been deleted correctly during crash recovery.
     *
     * @throws Exception If failed.
     */
    public void testPartitionFileDestroyCrashRecovery1() throws Exception {
        IgniteEx crd = startGrid(0);

        failFileIo = true;

        IgniteEx problemNode = startGrid(1);

        failFileIo = false;

        crd.cluster().active(true);

        int keysCnt = 50_000;

        loadData(crd, keysCnt, 1);

        startGridsMultiThreaded(2, 2);

        // Trigger partitions eviction.
        resetBaselineTopology();

        awaitPartitionMapExchange(true, true, null);

        checkPartitionFiles(problemNode, true);

        try {
            forceCheckpoint(problemNode);

            Assert.assertTrue("Checkpoint must be failed", false);
        }
        catch (Exception expected) {
            expected.printStackTrace();
        }

        // Problem node should be stopped after failed checkpoint.
        waitForTopology(3);

        problemNode = startGrid(1);

        awaitPartitionMapExchange();

        // After recovery all evicted partition files should be deleted from disk.
        checkPartitionFiles(problemNode, false);

        for (Ignite ignite : G.allGrids())
            checkData((IgniteEx) ignite, keysCnt, 1);
    }

    /**
     * Test that partitions files are not deleted if they were re-created on next time
     * and no checkpoint has done during this time.
     *
     * @throws Exception If failed.
     */
    public void testPartitionFileDestroyCrashRecovery2() throws Exception {
        IgniteEx crd = startGrid(0);

        failFileIo = true;

        IgniteEx problemNode = startGrid(1);

        failFileIo = false;

        crd.cluster().active(true);

        int keysCnt = 50_000;

        loadData(crd, keysCnt, 1);

        // Trigger partitions eviction.
        startGridsMultiThreaded(2, 2);

        resetBaselineTopology();

        awaitPartitionMapExchange(true, true, null);

        checkPartitionFiles(problemNode, true);

        // Trigger partitions re-create.
        stopGrid(2);

        resetBaselineTopology();

        awaitPartitionMapExchange(true, true, null);

        checkPartitionFiles(problemNode, true);

        try {
            forceCheckpoint(problemNode);

            Assert.assertTrue("Checkpoint must be failed", false);
        }
        catch (Exception expected) {
            expected.printStackTrace();
        }

        // Problem node should be stopped after failed checkpoint.
        waitForTopology(2);

        problemNode = startGrid(1);

        awaitPartitionMapExchange();

        // After recovery all evicted partition files should be deleted from disk.
        checkPartitionFiles(problemNode, false);

        for (Ignite ignite : G.allGrids())
            checkData((IgniteEx) ignite, keysCnt, 1);
    }

    /**
     * Test destroy when partition files are empty and there are no pages for checkpoint.
     *
     * @throws Exception If failed.
     */
    public void testDestroyWhenPartitionsAreEmpty() throws Exception {
        IgniteEx crd = (IgniteEx) startGrids(2);

        crd.cluster().active(true);

        forceCheckpoint();

        // Evict arbitrary partition.
        List<GridDhtLocalPartition> parts = crd.cachex(CACHE).context().topology().localPartitions();
        for (GridDhtLocalPartition part : parts)
            if (part.state() != GridDhtPartitionState.EVICTED) {
                part.rent(false).get();

                break;
            }

        // This checkpoint has no pages to write, but has one partition file to destroy.
        forceCheckpoint(crd);

        checkPartitionFiles(crd, false);
    }

    /**
     * If {@code exists} is {@code true}, checks that all partition files exist
     * if partition has state EVICTED.
     *
     * If {@code exists} is {@code false}, checks that all partition files don't exist
     * if partition is absent or has state EVICTED.
     *
     * @param ignite Node.
     * @param exists If {@code true} method will check that partition file exists,
     *               in other case will check that file doesn't exist.
     * @throws IgniteCheckedException If failed.
     */
    private void checkPartitionFiles(IgniteEx ignite, boolean exists) throws IgniteCheckedException {
        int evicted = 0;

        GridDhtPartitionTopology top = ignite.cachex(CACHE).context().topology();

        for (int p = 0; p < PARTS_CNT; p++) {
            GridDhtLocalPartition part = top.localPartition(p);

            File partFile = partitionFile(ignite, CACHE, p);

            if (exists) {
                if (part != null && part.state() == GridDhtPartitionState.EVICTED)
                    Assert.assertTrue("Partition file has deleted ahead of time: " + partFile, partFile.exists());

                evicted++;
            }
            else {
                if (part == null || part.state() == GridDhtPartitionState.EVICTED)
                    Assert.assertTrue("Partition file has not deleted: " + partFile, !partFile.exists());
            }
        }

        if (exists)
            Assert.assertTrue("There should be at least 1 eviction", evicted > 0);
    }

    /**
     * @param ignite Ignite.
     * @param cacheName Cache name.
     * @param partId Partition id.
     */
    private static File partitionFile(Ignite ignite, String cacheName, int partId) throws IgniteCheckedException {
        File dbDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false);

        String nodeName = ignite.name().replaceAll("\\.", "_");

        return new File(dbDir, String.format("%s/cache-%s/part-%d.bin", nodeName, cacheName, partId));
    }

    /**
     *
     */
    static class FailingFileIO extends FileIODecorator {
        /**
         * @param delegate File I/O delegate
         */
        public FailingFileIO(FileIO delegate) {
            super(delegate);
        }

        /** {@inheritDoc} */
        @Override public void clear() throws IOException {
            throw new IOException("Test");
        }
    }

    /**
     *
     */
    static class FailingFileIOFactory implements FileIOFactory {
        /** Delegate factory. */
        private final FileIOFactory delegateFactory;

        /**
         * @param delegateFactory Delegate factory.
         */
        FailingFileIOFactory(FileIOFactory delegateFactory) {
            this.delegateFactory = delegateFactory;
        }

        /**
         * @param file File.
         */
        private static boolean isPartitionFile(File file) {
            return file.getName().contains("part") && file.getName().endsWith("bin");
        }

        /** {@inheritDoc} */
        @Override public FileIO create(File file) throws IOException {
            FileIO delegate = delegateFactory.create(file);

            if (isPartitionFile(file))
                return new FailingFileIO(delegate);

            return delegate;
        }

        /** {@inheritDoc} */
        @Override public FileIO create(File file, OpenOption... modes) throws IOException {
            FileIO delegate = delegateFactory.create(file, modes);

            if (isPartitionFile(file))
                return new FailingFileIO(delegate);

            return delegate;
        }
    }
}
