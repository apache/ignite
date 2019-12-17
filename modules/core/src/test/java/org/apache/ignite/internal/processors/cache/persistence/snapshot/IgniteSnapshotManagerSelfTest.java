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
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointFuture;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIODecorator;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.FastCrc;
import org.apache.ignite.internal.processors.marshaller.MappedName;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static java.nio.file.Files.newDirectoryStream;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.FILE_SUFFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.PART_FILE_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.cacheDirName;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.cacheWorkDir;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.DELTA_SUFFIX;

/**
 * TODO backup must fail in case of parallel cache stop operation
 */
public class IgniteSnapshotManagerSelfTest extends GridCommonAbstractTest {
    /** */
    private static final FileIOFactory DFLT_IO_FACTORY = new RandomAccessFileIOFactory();

    /** */
    private static final String SNAPSHOT_NAME = "testSnapshot";

    /** */
    private static final int CACHE_PARTS_COUNT = 8;

    /** */
    private static final int PAGE_SIZE = 1024;

    /** */
    private static final int CACHE_KEYS_RANGE = 1024;

    /** */
    private static final PathMatcher DELTA_FILE_MATCHER =
        FileSystems.getDefault().getPathMatcher("glob:**" + DELTA_SUFFIX);

    /** */
    private static final DataStorageConfiguration memCfg = new DataStorageConfiguration()
        .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
            .setMaxSize(100L * 1024 * 1024)
            .setPersistenceEnabled(true))
        .setPageSize(PAGE_SIZE)
        .setWalMode(WALMode.LOG_ONLY);

    /** */
    private CacheConfiguration<Integer, Integer> defaultCacheCfg =
        new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME)
            .setCacheMode(CacheMode.PARTITIONED)
            .setRebalanceMode(CacheRebalanceMode.ASYNC)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setAffinity(new RendezvousAffinityFunction(false)
                .setPartitions(CACHE_PARTS_COUNT));

    /**
     * Calculate CRC for all partition files of specified cache.
     *
     * @param cacheDir Cache directory to iterate over partition files.
     * @return The map of [fileName, checksum].
     * @throws IgniteCheckedException If fails.
     */
    private static Map<String, Integer> calculateCRC32Partitions(File cacheDir) throws IgniteCheckedException {
        assert cacheDir.isDirectory() : cacheDir.getAbsolutePath();

        Map<String, Integer> result = new HashMap<>();

        try {
            try (DirectoryStream<Path> partFiles = newDirectoryStream(cacheDir.toPath(),
                p -> p.toFile().getName().startsWith(PART_FILE_PREFIX) && p.toFile().getName().endsWith(FILE_SUFFIX))
            ) {
                for (Path path : partFiles)
                    result.put(path.toFile().getName(), FastCrc.calcCrc(path.toFile()));
            }

            return result;
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }
    }

    /** */
    @Before
    public void beforeTestSnapshot() throws Exception {
        cleanPersistenceDir();
    }

    /** */
    @After
    public void afterTestSnapshot() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setConsistentId(igniteInstanceName)
            .setDataStorageConfiguration(memCfg)
            .setCacheConfiguration(defaultCacheCfg);
    }

    /**
     *
     */
    @Test
    public void testSnapshotLocalPartitions() throws Exception {
        // Start grid node with data before each test.
        IgniteEx ig = startGridWithCache(defaultCacheCfg, CACHE_KEYS_RANGE);

        for (int i = CACHE_KEYS_RANGE; i < 2048; i++)
            ig.cache(DEFAULT_CACHE_NAME).put(i, i);

        try (IgniteDataStreamer<Integer, TestOrderItem> ds = ig.dataStreamer(DEFAULT_CACHE_NAME)) {
            for (int i = 0; i < 2048; i++)
                ds.addData(i, new TestOrderItem(i, i));
        }

        try (IgniteDataStreamer<Integer, TestOrderItem> ds = ig.dataStreamer(DEFAULT_CACHE_NAME)) {
            for (int i = 0; i < 2048; i++)
                ds.addData(i, new TestOrderItem(i, i) {
                    @Override public String toString() {
                        return "_" + super.toString();
                    }
                });
        }

        IgniteSnapshotManager mgr = ig.context()
            .cache()
            .context()
            .snapshotMgr();

        IgniteInternalFuture<?> snpFut = mgr.createLocalSnapshot(SNAPSHOT_NAME,
            Collections.singletonList(CU.cacheId(DEFAULT_CACHE_NAME)));

        snpFut.get();

        File cacheWorkDir = ((FilePageStoreManager)ig.context()
            .cache()
            .context()
            .pageStore())
            .cacheWorkDir(defaultCacheCfg);

        // Calculate CRCs
        final Map<String, Integer> origParts = calculateCRC32Partitions(cacheWorkDir);

        String nodePath = mgr.relativeStoragePath();

        final Map<String, Integer> bakcupCRCs = calculateCRC32Partitions(
            Paths.get(mgr.localSnapshotDir(SNAPSHOT_NAME).getPath(), nodePath, cacheDirName(defaultCacheCfg)).toFile()
        );

        assertEquals("Partiton must have the same CRC after shapshot and after merge", origParts, bakcupCRCs);

        File snpWorkDir = mgr.snapshotWorkDir();

        assertEquals("Snapshot working directory must be cleand after usage", 0, snpWorkDir.listFiles().length);
    }

    /**
     *
     */
    @Test
    public void testSnapshotLocalPartitionsNextCpStarted() throws Exception {
        final int value_multiplier = 2;
        CountDownLatch slowCopy = new CountDownLatch(1);

        IgniteEx ig = startGridWithCache(defaultCacheCfg.setAffinity(new ZeroPartitionAffinityFunction()
            .setPartitions(CACHE_PARTS_COUNT)), CACHE_KEYS_RANGE);

        Set<Integer> ints = Stream.iterate(0, n -> n + 1)
            .limit(CACHE_PARTS_COUNT) // With index partition
            .collect(Collectors.toSet());
        ints.add(PageIdAllocator.INDEX_PARTITION);

        Map<Integer, GridIntList> parts = new HashMap<>();

        parts.put(CU.cacheId(DEFAULT_CACHE_NAME), GridIntList.valueOf(ints));

        FilePageStoreManager storeMgr = (FilePageStoreManager)ig.context()
            .cache()
            .context()
            .pageStore();

        IgniteSnapshotManager mgr = ig.context()
            .cache()
            .context()
            .snapshotMgr();

        File cacheWorkDir = storeMgr.cacheWorkDir(defaultCacheCfg);
        File cpDir = ((GridCacheDatabaseSharedManager) ig.context().cache().context().database())
            .checkpointDirectory();
        File walDir = ((FileWriteAheadLogManager) ig.context().cache().context().wal()).walWorkDir();
        File cacheBackup = cacheWorkDir(mgr.localSnapshotDir(SNAPSHOT_NAME), cacheDirName(defaultCacheCfg));

        // Change data before backup
        for (int i = 0; i < CACHE_KEYS_RANGE; i++)
            ig.cache(DEFAULT_CACHE_NAME).put(i, value_multiplier * i);

        File snapshotDir0 = mgr.localSnapshotDir(SNAPSHOT_NAME);

        IgniteInternalFuture<?> snpFut = mgr
            .scheduleSnapshot(SNAPSHOT_NAME,
                parts,
                mgr.snapshotExecutorService(),
                new DeleagateSnapshotSender(log, mgr.localSnapshotSender(snapshotDir0)) {
                    @Override
                    public void sendPart0(File part, String cacheDirName, GroupPartitionId pair, Long len) {
                        try {
                            if (pair.getPartitionId() == 0)
                                U.await(slowCopy);

                            delegate.sendPart0(part, cacheDirName, pair, len);
                        }
                        catch (IgniteInterruptedCheckedException e) {
                            throw new IgniteException(e);
                        }
                    }
                });

        // Change data after backup
        for (int i = 0; i < CACHE_KEYS_RANGE; i++)
            ig.cache(DEFAULT_CACHE_NAME).put(i, 3 * i);

        // Backup on the next checkpoint must copy page before write it to partition
        CheckpointFuture cpFut = ig.context()
            .cache()
            .context()
            .database()
            .forceCheckpoint("second cp");

        cpFut.finishFuture().get();

        slowCopy.countDown();

        snpFut.get();

        // Now can stop the node and check created backups.

        stopGrid(0);

        IgniteUtils.delete(cpDir);
        IgniteUtils.delete(walDir);

        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(0))
            .setWorkDirectory(mgr.localSnapshotDir(SNAPSHOT_NAME).getAbsolutePath());

        IgniteEx ig2 = startGrid(cfg);

        ig2.cluster().active(true);

        for (int i = 0; i < CACHE_KEYS_RANGE; i++)
            assertEquals(i * value_multiplier, ig2.cache(DEFAULT_CACHE_NAME).get(i));
    }

    /**
     *
     */
    @Test(expected = IgniteCheckedException.class)
    public void testSnapshotLocalPartitionNotEnoughSpace() throws Exception {
        final AtomicInteger throwCntr = new AtomicInteger();

        IgniteEx ig = startGridWithCache(defaultCacheCfg.setAffinity(new ZeroPartitionAffinityFunction()
            .setPartitions(CACHE_PARTS_COUNT)), CACHE_KEYS_RANGE);

        // Change data after backup
        for (int i = 0; i < CACHE_KEYS_RANGE; i++)
            ig.cache(DEFAULT_CACHE_NAME).put(i, 2 * i);

        IgniteSnapshotManager mgr = ig.context()
            .cache()
            .context()
            .snapshotMgr();

        mgr.ioFactory(new FileIOFactory() {
            @Override public FileIO create(File file, OpenOption... modes) throws IOException {
                FileIO fileIo = DFLT_IO_FACTORY.create(file, modes);

                if (file.getName().equals(IgniteSnapshotManager.getPartitionDeltaFileName(0)))
                    return new FileIODecorator(fileIo) {
                        @Override public int writeFully(ByteBuffer srcBuf) throws IOException {
                            if (throwCntr.incrementAndGet() == 3)
                                throw new IOException("Test exception. Not enough space.");

                            return super.writeFully(srcBuf);
                        }
                    };

                return fileIo;
            }
        });

        IgniteInternalFuture<?> snpFut = mgr.createLocalSnapshot(SNAPSHOT_NAME,
            Collections.singletonList(CU.cacheId(DEFAULT_CACHE_NAME)));

        snpFut.get();
    }

    /**
     *
     */
    @Test(expected = IgniteCheckedException.class)
    public void testSnapshotCreateLocalCopyPartitionFail() throws Exception {
        IgniteEx ig = startGridWithCache(defaultCacheCfg, CACHE_KEYS_RANGE);

        Map<Integer, GridIntList> parts = new HashMap<>();

        parts.computeIfAbsent(CU.cacheId(DEFAULT_CACHE_NAME), c -> new GridIntList(1))
            .add(0);

        IgniteSnapshotManager mgr = ig.context()
            .cache()
            .context()
            .snapshotMgr();

        File snpDir0 = new File(mgr.localSnapshotWorkDir(), SNAPSHOT_NAME);

        IgniteInternalFuture<?> fut = mgr.scheduleSnapshot(SNAPSHOT_NAME,
            parts,
            mgr.snapshotExecutorService(),
            new DeleagateSnapshotSender(log, mgr.localSnapshotSender(snpDir0)) {
                @Override public void sendPart0(File part, String cacheDirName, GroupPartitionId pair, Long len) {
                    if (pair.getPartitionId() == 0)
                        throw new IgniteException("Test. Fail to copy partition: " + pair);

                    delegate.sendPart0(part, cacheDirName, pair, len);
                }
            });

        fut.get();
    }

    /**
     * @throws Exception If fails.
     */
    @Test
    public void testSnapshotRemotePartitions() throws Exception {
        IgniteEx ig0 = startGrids(2);

        ig0.cluster().active(true);

        for (int i = 0; i < CACHE_KEYS_RANGE; i++)
            ig0.cache(DEFAULT_CACHE_NAME).put(i, i);

        CheckpointFuture cpFut = ig0.context()
            .cache()
            .context()
            .database()
            .forceCheckpoint("the next one");

        cpFut.finishFuture().get();

        IgniteSnapshotManager mgr0 = ig0.context()
            .cache()
            .context()
            .snapshotMgr();

        final CountDownLatch cancelLatch = new CountDownLatch(1);

        mgr0.addSnapshotListener(new SnapshotListener() {
            @Override public void onPartition(UUID rmtNodeId, File part, int grpId, int partId) {
                log.info("Snapshot partition received successfully [rmtNodeId=" + rmtNodeId +
                    ", part=" + part.getAbsolutePath() + ", grpId=" + grpId + ", partId=" + partId + ']');

                cancelLatch.countDown();
            }

            @Override public void onEnd(UUID rmtNodeId) {
                log.info("Snapshot created successfully [rmtNodeId=" + rmtNodeId + ']');
            }

            @Override public void onException(UUID rmtNodeId, Throwable t) {
                fail("Exception must not be thrown [rmtNodeId=" + rmtNodeId + ", t=" + t);
            }
        });

        UUID rmtNodeId = grid(1).localNode().id();

        // Snapshot must be taken on node1 and transmitted to node0.
        IgniteInternalFuture<?> fut = mgr0.createRemoteSnapshot(rmtNodeId,
            owningParts(ig0, new HashSet<>(Collections.singletonList(CU.cacheId(DEFAULT_CACHE_NAME))), rmtNodeId));

        cancelLatch.await();

        fut.cancel();

        System.out.println(">>>> cancelled, started new");

        IgniteInternalFuture<?> fut2 = mgr0.createRemoteSnapshot(rmtNodeId,
            owningParts(ig0, new HashSet<>(Collections.singletonList(CU.cacheId(DEFAULT_CACHE_NAME))), rmtNodeId));

        fut2.get();
    }

    /**
     * @throws Exception If fails.
     */
    @Test
    public void testSnapshotRemoteOnBothNodes() throws Exception {
        IgniteEx ig0 = startGrids(2);

        ig0.cluster().active(true);

        for (int i = 0; i < CACHE_KEYS_RANGE; i++)
            ig0.cache(DEFAULT_CACHE_NAME).put(i, i);

        CheckpointFuture cpFut = ig0.context()
            .cache()
            .context()
            .database()
            .forceCheckpoint("the next one");

        cpFut.finishFuture().get();

        IgniteSnapshotManager mgr0 = ig0.context()
            .cache()
            .context()
            .snapshotMgr();

        IgniteSnapshotManager mgr1 = grid(1).context()
            .cache()
            .context()
            .snapshotMgr();

        UUID node0 = grid(0).localNode().id();
        UUID node1 = grid(1).localNode().id();

        // Snapshot must be taken on node1 and transmitted to node0.
        IgniteInternalFuture<?> futFrom1To0 = mgr0.createRemoteSnapshot(node1,
            owningParts(ig0, new HashSet<>(Collections.singletonList(CU.cacheId(DEFAULT_CACHE_NAME))), node1));

        IgniteInternalFuture<?> futFrom0To1 = mgr1.createRemoteSnapshot(node0,
            owningParts(grid(1), new HashSet<>(Collections.singletonList(CU.cacheId(DEFAULT_CACHE_NAME))), node0));

        futFrom0To1.get();
        futFrom1To0.get();
    }

    /**
     * @param src Source node to calculate.
     * @param grps Groups to collect owning parts.
     * @param rmtNodeId Remote node id.
     * @return Map of collected parts.
     */
    private static Map<Integer, Set<Integer>> owningParts(IgniteEx src, Set<Integer> grps, UUID rmtNodeId) {
        Map<Integer, Set<Integer>> result = new HashMap<>();

        for (Integer grpId : grps) {
            Set<Integer> parts = src.context()
                .cache()
                .cacheGroup(grpId)
                .topology()
                .partitions(rmtNodeId)
                .entrySet()
                .stream()
                .filter(p -> p.getValue() == GridDhtPartitionState.OWNING)
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());

            result.put(grpId, parts);
        }

        return result;
    }

    /**
     * @param ccfg Default cache configuration.
     * @return Ignite instance.
     * @throws Exception If fails.
     */
    private IgniteEx startGridWithCache(CacheConfiguration<Integer, Integer> ccfg, int range) throws Exception {
        defaultCacheCfg = ccfg;

        // Start grid node with data before each test.
        IgniteEx ig = startGrid(0);

        ig.cluster().active(true);

        for (int i = 0; i < range; i++)
            ig.cache(DEFAULT_CACHE_NAME).put(i, i);

        CheckpointFuture cpFut = ig.context()
            .cache()
            .context()
            .database()
            .forceCheckpoint("the next one");

        cpFut.finishFuture().get();

        return ig;
    }

    /**
     *
     */
    private static class ZeroPartitionAffinityFunction extends RendezvousAffinityFunction {
        @Override public int partition(Object key) {
            return 0;
        }
    }

    /**
     *
     */
    private static class DeleagateSnapshotSender extends SnapshotSender {
        /** Delegate call to. */
        protected final SnapshotSender delegate;

        /**
         * @param delegate Delegate call to.
         */
        public DeleagateSnapshotSender(IgniteLogger log, SnapshotSender delegate) {
            super(log);

            this.delegate = delegate;
        }

        /** {@inheritDoc} */
        @Override public void sendCacheConfig0(File ccfg, String cacheDirName, GroupPartitionId pair) {
            delegate.sendCacheConfig(ccfg, cacheDirName, pair);
        }

        /** {@inheritDoc} */
        @Override public void sendMarshallerMeta0(List<Map<Integer, MappedName>> mappings) {
            delegate.sendMarshallerMeta(mappings);
        }

        /** {@inheritDoc} */
        @Override public void sendBinaryMeta0(Map<Integer, BinaryType> types) {
            delegate.sendBinaryMeta(types);
        }

        /** {@inheritDoc} */
        @Override public void sendPart0(File part, String cacheDirName, GroupPartitionId pair, Long len) {
            delegate.sendPart(part, cacheDirName, pair, len);
        }

        /** {@inheritDoc} */
        @Override public void sendDelta0(File delta, String cacheDirName, GroupPartitionId pair) {
            delegate.sendDelta(delta, cacheDirName, pair);
        }

        /** {@inheritDoc} */
        @Override public void close0() throws IOException{
            delegate.close();
        }
    }

    /**
     *
     */
    private static class TestOrderItem implements Serializable {
        /** Order key. */
        private final int key;

        /** Order value. */
        private final int value;

        public TestOrderItem(int key, int value) {
            this.key = key;
            this.value = value;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TestOrderItem item = (TestOrderItem)o;

            return key == item.key &&
                value == item.value;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(key, value);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "TestOrderItem [key=" + key + ", value=" + value + ']';
        }
    }
}
