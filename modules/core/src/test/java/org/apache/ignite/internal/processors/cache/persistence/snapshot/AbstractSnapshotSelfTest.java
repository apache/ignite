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
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.discovery.CustomMessageWrapper;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.FastCrc;
import org.apache.ignite.internal.processors.marshaller.MappedName;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;

import static java.nio.file.Files.newDirectoryStream;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.configuration.IgniteConfiguration.DFLT_SNAPSHOT_DIRECTORY;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.FILE_SUFFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.PART_FILE_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.DFLT_SNAPSHOT_TMP_DIR;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.snapshotPath;

/**
 * Base snapshot tests.
 */
public abstract class AbstractSnapshotSelfTest extends GridCommonAbstractTest {
    /** Default snapshot name. */
    protected static final String SNAPSHOT_NAME = "testSnapshot";

    /** Default number of partitions for cache. */
    protected static final int CACHE_PARTS_COUNT = 8;

    /** Number of cache keys to pre-create at node start. */
    protected static final int CACHE_KEYS_RANGE = 1024;

    /** Configuration for the 'default' cache. */
    protected CacheConfiguration<Integer, Integer> dfltCacheCfg =
        new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME)
            .setCacheMode(CacheMode.PARTITIONED)
            .setRebalanceMode(CacheRebalanceMode.ASYNC)
            .setBackups(1)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setAffinity(new RendezvousAffinityFunction(false)
                .setPartitions(CACHE_PARTS_COUNT));

    /** {@inheritDoc} */
    @Override protected void cleanPersistenceDir() throws Exception {
        super.cleanPersistenceDir();

        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_SNAPSHOT_DIRECTORY, false));
    }

    /** @throws Exception If fails. */
    @Before
    public void beforeTestSnapshot() throws Exception {
        cleanPersistenceDir();
    }

    /** @throws Exception If fails. */
    @After
    public void afterTestSnapshot() throws Exception {
        try {
            for (Ignite ig : G.allGrids()) {
                if (ig.configuration().isClientMode())
                    continue;

                File storeWorkDir = ((FilePageStoreManager)((IgniteEx)ig).context()
                    .cache().context().pageStore()).workDir();

                Path snpTempDir = Paths.get(storeWorkDir.getAbsolutePath(), DFLT_SNAPSHOT_TMP_DIR);

                assertTrue("Snapshot working directory must be empty at the moment test execution stopped: " + snpTempDir,
                    F.isEmptyDirectory(snpTempDir));
            }
        }
        finally {
            stopAllGrids();
        }

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi discoSpi = new BlockingCustomMessageDiscoverySpi();

        discoSpi.setIpFinder(((TcpDiscoverySpi)cfg.getDiscoverySpi()).getIpFinder());

        return cfg.setConsistentId(igniteInstanceName)
            .setCommunicationSpi(new TestRecordingCommunicationSpi())
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setMaxSize(100L * 1024 * 1024)
                    .setPersistenceEnabled(true))
                .setCheckpointFrequency(3000)
                .setPageSize(4096))
            .setCacheConfiguration(dfltCacheCfg)
            .setClusterStateOnStart(INACTIVE)
            // Default work directory must be resolved earlier if snapshot used to start grids.
            .setWorkDirectory(U.defaultWorkDirectory())
            .setDiscoverySpi(discoSpi);
    }

    /**
     * Calculate CRC for all partition files of specified cache.
     *
     * @param cacheDir Cache directory to iterate over partition files.
     * @return The map of [fileName, checksum].
     */
    public static Map<String, Integer> calculateCRC32Partitions(File cacheDir) {
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
            throw new IgniteException(e);
        }
    }

    /**
     * @param path Directory to search.
     * @param dir Directory name.
     * @return Result.
     * @throws IOException If fails.
     */
    public static Optional<Path> searchDirectoryRecursively(Path path, String dir) throws IOException {
        if (Files.notExists(path))
            return Optional.empty();

        return  Files.walk(path)
            .filter(Files::isDirectory)
            .filter(file -> dir.equals(file.getFileName().toString()))
            .findAny();
    }

    /**
     * @param ccfg Default cache configuration.
     * @return Ignite instance.
     * @throws Exception If fails.
     */
    protected IgniteEx startGridWithCache(CacheConfiguration<Integer, Integer> ccfg, int range) throws Exception {
        return startGridsWithCache(1, ccfg, range);
    }

    /**
     * @param cnt Number of grids to start.
     * @param ccfg Default cache configuration.
     * @param range Range of cache keys to insert.
     * @return Ignite instance.
     * @throws Exception If fails.
     */
    protected IgniteEx startGridsWithCache(int cnt, CacheConfiguration<Integer, Integer> ccfg, int range) throws Exception {
        dfltCacheCfg = ccfg;

        // Start grid node with data before each test.
        IgniteEx ig = startGrids(cnt);

        ig.cluster().baselineAutoAdjustEnabled(false);
        ig.cluster().state(ClusterState.ACTIVE);

        for (int i = 0; i < range; i++)
            ig.cache(DEFAULT_CACHE_NAME).put(i, i);

        forceCheckpoint();

        return ig;
    }

    /**
     * @param cnt Number of grids to start.
     * @param snpName Snapshot to start grids from.
     * @return Coordinator ignite instance.
     * @throws Exception If fails.
     */
    protected IgniteEx startGridsFromSnapshot(int cnt, String snpName) throws Exception {
        return startGridsFromSnapshot(cnt, cfg -> snapshotPath(cfg).toString(), snpName);
    }

    /**
     * @param cnt Number of grids to start.
     * @param path Snapshot path resolver.
     * @param snpName Snapshot to start grids from.
     * @return Coordinator ignite instance.
     * @throws Exception If fails.
     */
    protected IgniteEx startGridsFromSnapshot(int cnt,
        Function<IgniteConfiguration, String> path,
        String snpName
    ) throws Exception {
        IgniteEx crd = null;

        for (int i = 0; i < cnt; i++) {
            IgniteConfiguration cfg = optimize(getConfiguration(getTestIgniteInstanceName(i)));

            cfg.setWorkDirectory(Paths.get(path.apply(cfg), snpName).toString());

            if (crd == null)
                crd = startGrid(cfg);
            else
                startGrid(cfg);
        }

        crd.cluster().baselineAutoAdjustEnabled(false);
        crd.cluster().state(ACTIVE);

        return crd;
    }

    /**
     * @param ignite Ignite instance.
     * @return Snapshot manager related to given ignite instance.
     */
    public static IgniteSnapshotManager snp(IgniteEx ignite) {
        return ignite.context().cache().context().snapshotMgr();
    }

    /**
     * @param ignite Ignite instance.
     * @return Directory name for ignite instance.
     * @throws IgniteCheckedException If fails.
     */
    public static String folderName(IgniteEx ignite) throws IgniteCheckedException {
        return ignite.context().pdsFolderResolver().resolveFolders().folderName();
    }

    /**
     * @param cache Ignite cache to check.
     */
    protected static void assertSnapshotCacheKeys(IgniteCache<?, ?> cache) {
        List<Integer> keys = IntStream.range(0, CACHE_KEYS_RANGE).boxed().collect(Collectors.toList());

        cache.query(new ScanQuery<>(null))
            .forEach(e -> keys.remove((Integer)e.getKey()));

        assertTrue("Snapshot must contains pre-created cache data " +
            "[cache=" + cache.getName() + ", keysLeft=" + keys +']', keys.isEmpty());
    }

    /**
     * @param ignite Ignite instance to resolve discovery spi to.
     * @return BlockingCustomMessageDiscoverySpi instance.
     */
    protected static BlockingCustomMessageDiscoverySpi discoSpi(IgniteEx ignite) {
        return (BlockingCustomMessageDiscoverySpi)ignite.context().discovery().getInjectedDiscoverySpi();
    }

    /** */
    protected static class BlockingCustomMessageDiscoverySpi extends TcpDiscoverySpi {
        /** List of messages which have been blocked. */
        private final List<DiscoverySpiCustomMessage> blocked = new CopyOnWriteArrayList<>();

        /** Discovery custom message filter. */
        private volatile IgnitePredicate<DiscoveryCustomMessage> blockPred;

        /** {@inheritDoc} */
        @Override public void sendCustomEvent(DiscoverySpiCustomMessage msg) throws IgniteException {
            if (msg instanceof CustomMessageWrapper) {
                DiscoveryCustomMessage msg0 = ((CustomMessageWrapper)msg).delegate();

                if (blockPred != null && blockPred.apply(msg0)) {
                    blocked.add(msg);

                    if (log.isInfoEnabled())
                        log.info("Discovery message has been blocked: " + msg0);

                    return;
                }
            }

            super.sendCustomEvent(msg);
        }

        /** Start blocking discovery custom messages. */
        public synchronized void block(IgnitePredicate<DiscoveryCustomMessage> pred) {
            blockPred = pred;
        }

        /** Unblock and send previously saved discovery custom messages */
        public synchronized void unblock() {
            blockPred = null;

            for (DiscoverySpiCustomMessage msg : blocked)
                sendCustomEvent(msg);

            blocked.clear();
        }

        /**
         * @param timeout Timeout to wait blocking messages.
         * @throws IgniteInterruptedCheckedException If interrupted.
         */
        public void waitBlocked(long timeout) throws IgniteInterruptedCheckedException {
            GridTestUtils.waitForCondition(() -> !blocked.isEmpty(), timeout);
        }
    }

    /** */
    protected static class DelegateSnapshotSender extends SnapshotSender {
        /** Delegate call to. */
        protected final SnapshotSender delegate;

        /**
         * @param delegate Delegate call to.
         */
        public DelegateSnapshotSender(IgniteLogger log, Executor exec, SnapshotSender delegate) {
            super(log, exec);

            this.delegate = delegate;
        }

        /** {@inheritDoc} */
        @Override protected void init(int partsCnt) {
            delegate.init(partsCnt);
        }

        /** {@inheritDoc} */
        @Override public void sendCacheConfig0(File ccfg, String cacheDirName) {
            delegate.sendCacheConfig(ccfg, cacheDirName);
        }

        /** {@inheritDoc} */
        @Override public void sendMarshallerMeta0(List<Map<Integer, MappedName>> mappings) {
            delegate.sendMarshallerMeta(mappings);
        }

        /** {@inheritDoc} */
        @Override public void sendBinaryMeta0(Collection<BinaryType> types) {
            delegate.sendBinaryMeta(types);
        }

        /** {@inheritDoc} */
        @Override public void sendPart0(File part, String cacheDirName, GroupPartitionId pair, Long length) {
            delegate.sendPart(part, cacheDirName, pair, length);
        }

        /** {@inheritDoc} */
        @Override public void sendDelta0(File delta, String cacheDirName, GroupPartitionId pair) {
            delegate.sendDelta(delta, cacheDirName, pair);
        }

        /** {@inheritDoc} */
        @Override public void close0(Throwable th) {
            delegate.close(th);
        }
    }
}
