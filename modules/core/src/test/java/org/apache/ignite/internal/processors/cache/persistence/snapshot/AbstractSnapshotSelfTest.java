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
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.encryption.AbstractEncryptionTest;
import org.apache.ignite.internal.managers.discovery.CustomMessageWrapper;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.cache.CacheGroupDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.FastCrc;
import org.apache.ignite.internal.processors.marshaller.MappedName;
import org.apache.ignite.internal.processors.resource.GridSpringResourceContext;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteFutureCancelledException;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.encryption.keystore.KeystoreEncryptionSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.nio.file.Files.newDirectoryStream;
import static org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction.DFLT_PARTITION_COUNT;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_PAGE_SIZE;
import static org.apache.ignite.events.EventType.EVTS_CLUSTER_SNAPSHOT;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.FILE_SUFFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.PART_FILE_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.CP_SNAPSHOT_REASON;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.DFLT_SNAPSHOT_TMP_DIR;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.resolveSnapshotWorkDirectory;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Base snapshot tests.
 */
@RunWith(Parameterized.class)
public abstract class AbstractSnapshotSelfTest extends GridCommonAbstractTest {
    /** Default snapshot name. */
    public static final String SNAPSHOT_NAME = "testSnapshot";

    /** Number of cache keys to pre-create at node start. */
    protected static final int CACHE_KEYS_RANGE = 1024;

    /** Number of partitions within a snapshot cache group. */
    protected static final int CACHE_PARTITIONS_COUNT = GridTestUtils.SF.apply(DFLT_PARTITION_COUNT);

    /** Timeout in milliseconds to await for snapshot operation being completed. */
    protected static final long TIMEOUT = 15_000;

    /** List of collected snapshot test events. */
    protected final List<Integer> locEvts = new CopyOnWriteArrayList<>();

    /** Configuration for the 'default' cache. */
    protected volatile CacheConfiguration<Integer, Object> dfltCacheCfg;

    /** Enable default data region persistence. */
    protected boolean persistence = true;

    /** Master key name. */
    protected String masterKeyName;

    /** Cache value builder. */
    protected Function<Integer, Object> valBuilder = String::valueOf;

    /**
     * @return Cache value builder.
     */
    protected Function<Integer, Object> valueBuilder() {
        return valBuilder;
    }

    /** Enable encryption of all caches in {@code IgniteConfiguration} before start. */
    @Parameterized.Parameter
    public boolean encryption;

    /** Parameters. */
    @Parameterized.Parameters(name = "Encryption={0}")
    public static Collection<Boolean> encryptionParams() {
        return Arrays.asList(false, true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi discoSpi = new BlockingCustomMessageDiscoverySpi();

        discoSpi.setIpFinder(((TcpDiscoverySpi)cfg.getDiscoverySpi()).getIpFinder());

        if (dfltCacheCfg != null)
            cfg.setCacheConfiguration(dfltCacheCfg);

        return cfg.setConsistentId(igniteInstanceName)
            .setCommunicationSpi(new TestRecordingCommunicationSpi())
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setMaxSize(100L * 1024 * 1024)
                    .setPersistenceEnabled(persistence))
                .setCheckpointFrequency(3000)
                .setPageSize(DFLT_PAGE_SIZE))
            .setClusterStateOnStart(INACTIVE)
            .setIncludeEventTypes(EVTS_CLUSTER_SNAPSHOT)
            .setDiscoverySpi(discoSpi);
    }

    /** {@inheritDoc} */
    @Override protected Ignite startGrid(String igniteInstanceName, IgniteConfiguration cfg,
        GridSpringResourceContext ctx) throws Exception {

        if (encryption && persistence) {
            KeystoreEncryptionSpi encSpi = new KeystoreEncryptionSpi();

            encSpi.setKeyStorePath(AbstractEncryptionTest.KEYSTORE_PATH);
            encSpi.setKeyStorePassword(AbstractEncryptionTest.KEYSTORE_PASSWORD.toCharArray());

            if (masterKeyName != null)
                encSpi.setMasterKeyName(masterKeyName);

            cfg.setEncryptionSpi(encSpi);

            if (cfg.getCacheConfiguration() != null) {
                for (CacheConfiguration<?, ?> cacheCfg : cfg.getCacheConfiguration())
                    cacheCfg.setEncryptionEnabled(true);
            }
        }

        return super.startGrid(igniteInstanceName, cfg, ctx);
    }

    /** @throws Exception If fails. */
    @Before
    public void beforeTestSnapshot() throws Exception {
        cleanPersistenceDir();

        dfltCacheCfg = txCacheConfig(new CacheConfiguration<>(DEFAULT_CACHE_NAME));
        locEvts.clear();
    }

    /** @throws Exception If fails. */
    @After
    public void afterTestSnapshot() throws Exception {
        try {
            for (Ignite ig : G.allGrids()) {
                if (ig.configuration().isClientMode() || !persistence)
                    continue;

                File storeWorkDir = ((FilePageStoreManager)((IgniteEx)ig).context()
                    .cache().context().pageStore()).workDir();

                Path snpTempDir = Paths.get(storeWorkDir.getAbsolutePath(), DFLT_SNAPSHOT_TMP_DIR);

                assertEquals("Snapshot working directory must be empty at the moment test execution stopped: " + snpTempDir,
                    0, U.fileCount(snpTempDir));
            }
        }
        finally {
            stopAllGrids();
        }

        cleanPersistenceDir();
    }

    /**
     * @param evts Events to check.
     * @throws IgniteInterruptedCheckedException If interrupted.
     */
    protected void waitForEvents(Integer... evts) throws IgniteInterruptedCheckedException {
        boolean caught = waitForCondition(() -> locEvts.containsAll(Arrays.asList(evts)), TIMEOUT);

        assertTrue("Events must be caught [locEvts=" + locEvts + ']', caught);
    }

    /**
     * @param ccfg Ensures the cache is absent.
     * @throws IgniteCheckedException if failed.
     */
    protected void ensureCacheAbsent(CacheConfiguration<?, ?> ccfg) throws IgniteCheckedException, InterruptedException {
        String cacheName = ccfg.getName();

        for (Ignite ignite : G.allGrids()) {
            GridKernalContext kctx = ((IgniteEx)ignite).context();

            if (kctx.clientNode())
                continue;

            CacheGroupDescriptor desc = kctx.cache().cacheGroupDescriptors().get(CU.cacheId(cacheName));

            assertNull("nodeId=" + kctx.localNodeId() + ", cache=" + cacheName, desc);

            boolean success = GridTestUtils.waitForCondition(
                () -> !kctx.cache().context().snapshotMgr().isRestoring(),
                TIMEOUT);

            assertTrue("The process has not finished on the node " + kctx.localNodeId(), success);

            File dir = ((FilePageStoreManager)kctx.cache().context().pageStore()).cacheWorkDir(ccfg);

            String errMsg = String.format("%s, dir=%s, exists=%b, files=%s",
                ignite.name(), dir, dir.exists(), Arrays.toString(dir.list()));

            assertTrue(errMsg, !dir.exists() || dir.list().length == 0);
        }
    }

    /**
     * @param ccfg Default cache configuration.
     * @return Cache configuration.
     */
    protected <K, V> CacheConfiguration<K, V> txCacheConfig(CacheConfiguration<K, V> ccfg) {
        return ccfg.setCacheMode(CacheMode.PARTITIONED)
            .setBackups(2)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setAffinity(new RendezvousAffinityFunction(false, CACHE_PARTITIONS_COUNT))
            .setEncryptionEnabled(encryption);
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

        return Files.walk(path)
            .filter(Files::isDirectory)
            .filter(file -> dir.equals(file.getFileName().toString()))
            .findAny();
    }

    /**
     * @param ccfg Default cache configuration.
     * @return Ignite instance.
     * @throws Exception If fails.
     */
    protected IgniteEx startGridWithCache(CacheConfiguration<Integer, Object> ccfg, int keys) throws Exception {
        return startGridsWithCache(1, ccfg, keys);
    }

    /**
     * @param grids Number of grids to start.
     * @param ccfg Default cache configuration.
     * @param keys Range of cache keys to insert.
     * @return Ignite instance.
     * @throws Exception If fails.
     */
    protected IgniteEx startGridsWithCache(int grids, CacheConfiguration<Integer, Object> ccfg, int keys) throws Exception {
        dfltCacheCfg = ccfg;

        return startGridsWithCache(grids, keys, Integer::new, ccfg);
    }

    /**
     * @param grids Number of ignite instances to start.
     * @param keys Number of keys to create.
     * @param factory Factory which produces values.
     * @param <V> Cache value type.
     * @return Ignite coordinator instance.
     * @throws Exception If fails.
     */
    protected <V> IgniteEx startGridsWithCache(
        int grids,
        int keys,
        Function<Integer, V> factory,
        CacheConfiguration<Integer, V>... ccfgs
    ) throws Exception {
        return startGridsWithCache(grids, keys, factory, (id, cfg) -> cfg.getWorkDirectory(), ccfgs);
    }

    /**
     * @param grids Number of ignite instances to start.
     * @param keys Number of keys to create.
     * @param factory Factory which produces values.
     * @param <V> Cache value type.
     * @return Ignite coordinator instance.
     * @throws Exception If fails.
     */
    protected <V> IgniteEx startGridsWithCache(
        int grids,
        int keys,
        Function<Integer, V> factory,
        BiFunction<Integer, IgniteConfiguration, String> newWorkDir,
        CacheConfiguration<Integer, V>... ccfgs
    ) throws Exception {
        for (int g = 0; g < grids; g++) {
            IgniteConfiguration cfg = optimize(getConfiguration(getTestIgniteInstanceName(g))
                .setCacheConfiguration(ccfgs));

            cfg.setWorkDirectory(newWorkDir.apply(g, cfg));

            startGrid(cfg);
        }

        IgniteEx ig = grid(0);

        ig.cluster().baselineAutoAdjustEnabled(false);
        ig.cluster().state(ClusterState.ACTIVE);

        for (CacheConfiguration<Integer, V> ccfg : ccfgs) {
            try (IgniteDataStreamer<Integer, V> ds = ig.dataStreamer(ccfg.getName())) {
                for (int i = 0; i < keys; i++)
                    ds.addData(i, factory.apply(i));
            }
        }

        forceCheckpoint();

        ig.events().localListen(e -> locEvts.add(e.type()), EVTS_CLUSTER_SNAPSHOT);

        return ig;
    }

    /**
     * @param grids Number of ignite instances.
     * @return Coordinator ignite instance.
     * @throws Exception If fails.
     */
    protected IgniteEx startGridsWithoutCache(int grids) throws Exception {
        for (int i = 0; i < grids; i++)
            startGrid(optimize(getConfiguration(getTestIgniteInstanceName(i)).setCacheConfiguration()));

        IgniteEx ignite = grid(0);

        ignite.cluster().baselineAutoAdjustEnabled(false);
        ignite.cluster().state(ClusterState.ACTIVE);

        return ignite;
    }

    /**
     * @param cnt Number of grids to start.
     * @param snpName Snapshot to start grids from.
     * @return Coordinator ignite instance.
     * @throws Exception If fails.
     */
    protected IgniteEx startGridsFromSnapshot(int cnt, String snpName) throws Exception {
        return startGridsFromSnapshot(cnt, cfg -> resolveSnapshotWorkDirectory(cfg).getAbsolutePath(), snpName, true);
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
        String snpName,
        boolean activate
    ) throws Exception {
        return startGridsFromSnapshot(IntStream.range(0, cnt).boxed().collect(Collectors.toSet()), path, snpName, activate);
    }

    /**
     * @param ids Set of ignite instances ids to start.
     * @param path Snapshot path resolver.
     * @param snpName Snapshot to start grids from.
     * @param activate {@code true} to activate after cluster start.
     * @return Coordinator ignite instance.
     * @throws Exception If fails.
     */
    protected IgniteEx startGridsFromSnapshot(Set<Integer> ids,
        Function<IgniteConfiguration, String> path,
        String snpName,
        boolean activate
    ) throws Exception {
        IgniteEx crd = null;

        for (Integer i : ids) {
            IgniteConfiguration cfg = optimize(getConfiguration(getTestIgniteInstanceName(i)));

            cfg.setWorkDirectory(Paths.get(path.apply(cfg), snpName).toString());

            if (crd == null)
                crd = startGrid(cfg);
            else
                startGrid(cfg);
        }

        crd.cluster().baselineAutoAdjustEnabled(false);

        if (activate)
            crd.cluster().state(ACTIVE);

        return crd;
    }

    /**
     * @param nodesCnt Nodes count.
     * @param keysCnt Number of keys to create.
     * @param startClient {@code True} to start an additional client node.
     * @return Ignite coordinator instance.
     * @throws Exception if failed.
     */
    protected IgniteEx startGridsWithSnapshot(int nodesCnt, int keysCnt, boolean startClient) throws Exception {
        IgniteEx ignite = startGridsWithCache(nodesCnt, keysCnt, valueBuilder(), dfltCacheCfg);

        if (startClient)
            ignite = startClientGrid("client");

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get(TIMEOUT);

        ignite.cache(dfltCacheCfg.getName()).destroy();

        awaitPartitionMapExchange();

        return ignite;
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
            "[cache=" + cache.getName() + ", keysLeft=" + keys + ']', keys.isEmpty());
    }

    /**
     * @param cache Cache.
     * @param keysCnt Expected number of keys.
     */
    protected void assertCacheKeys(IgniteCache<Object, Object> cache, int keysCnt) {
        assertEquals(keysCnt, cache.size());

        for (int i = 0; i < keysCnt; i++)
            assertEquals(valueBuilder().apply(i), cache.get(i));
    }

    /**
     * @param grids Grids to block snapshot executors.
     * @return Wrapped snapshot executor list.
     */
    protected static List<BlockingExecutor> setBlockingSnapshotExecutor(List<? extends Ignite> grids) {
        List<BlockingExecutor> execs = new ArrayList<>();

        for (Ignite grid : grids) {
            IgniteSnapshotManager mgr = snp((IgniteEx)grid);
            BiFunction<String, String, SnapshotSender> old = mgr.localSnapshotSenderFactory();

            BlockingExecutor block = new BlockingExecutor(mgr.snapshotExecutorService());
            execs.add(block);

            mgr.localSnapshotSenderFactory((snpName, snpPath) ->
                new DelegateSnapshotSender(log, block, old.apply(snpName, snpPath)));
        }

        return execs;
    }

    /**
     * @param startCli Client node to start snapshot.
     * @param srvs Server nodes.
     * @param cache Persisted cache.
     * @param snpCanceller Snapshot cancel closure.
     */
    public static void doSnapshotCancellationTest(
        IgniteEx startCli,
        List<IgniteEx> srvs,
        IgniteCache<?, ?> cache,
        Consumer<String> snpCanceller
    ) {
        IgniteEx srv = srvs.get(0);

        CacheConfiguration<?, ?> ccfg = cache.getConfiguration(CacheConfiguration.class);

        assertTrue(CU.isPersistenceEnabled(srv.configuration()));
        assertTrue(CU.isPersistentCache(ccfg, srv.configuration().getDataStorageConfiguration()));

        File snpDir = resolveSnapshotWorkDirectory(srv.configuration());

        List<BlockingExecutor> execs = setBlockingSnapshotExecutor(srvs);

        IgniteFuture<Void> fut = startCli.snapshot().createSnapshot(SNAPSHOT_NAME);

        for (BlockingExecutor exec : execs)
            exec.waitForBlocked(30_000L);

        snpCanceller.accept(SNAPSHOT_NAME);

        assertThrowsAnyCause(log,
            fut::get,
            IgniteFutureCancelledException.class,
            "Execution of snapshot tasks has been cancelled by external process");

        assertEquals("Snapshot directory must be empty due to snapshot cancelled", 0, snpDir.list().length);
    }

    /**
     * @param snpName Unique snapshot name.
     * @param parts Collection of pairs group and appropriate cache partition to be snapshot.
     * @param snpSndr Sender which used for snapshot sub-task processing.
     * @return Future which will be completed when snapshot is done.
     */
    protected static IgniteInternalFuture<?> startLocalSnapshotTask(
        GridCacheSharedContext<?, ?> cctx,
        String snpName,
        Map<Integer, Set<Integer>> parts,
        boolean withMetaStorage,
        SnapshotSender snpSndr
    ) throws IgniteCheckedException {
        AbstractSnapshotFutureTask<?> task = cctx.snapshotMgr().registerSnapshotTask(snpName, cctx.localNodeId(), null,
            parts, withMetaStorage, snpSndr);

        if (!(task instanceof SnapshotFutureTask))
            throw new IgniteCheckedException("Snapshot task hasn't been registered: " + task);

        SnapshotFutureTask snpFutTask = (SnapshotFutureTask)task;

        snpFutTask.start();

        // Snapshot is still in the INIT state. beforeCheckpoint has been skipped
        // due to checkpoint already running and we need to schedule the next one
        // right after current will be completed.
        cctx.database().forceCheckpoint(String.format(CP_SNAPSHOT_REASON, snpName));

        snpFutTask.started().get();

        return snpFutTask;
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
    protected static class Value {
        /** */
        private final byte[] arr;

        /**
         * @param arr Test array.
         */
        public Value(byte[] arr) {
            this.arr = arr;
        }

        /** */
        public byte[] arr() {
            return arr;
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

    /** Account item. */
    protected static class Account implements Serializable {
        /** Serial version. */
        private static final long serialVersionUID = 0L;

        /** User id. */
        @QuerySqlField(index = true)
        @GridToStringInclude
        protected final int id;

        /** Order value. */
        @QuerySqlField
        @GridToStringInclude
        protected int balance;

        /**
         * @param id User id.
         * @param balance User balance.
         */
        public Account(int id, int balance) {
            this.id = id;
            this.balance = balance;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Account item = (Account)o;

            return id == item.id &&
                balance == item.balance;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(id, balance);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Account.class, this);
        }
    }

    /** */
    protected static class BlockingExecutor implements Executor {
        /** Delegate executor. */
        private final Executor delegate;

        /** Waiting tasks. */
        private final Queue<Runnable> tasks = new ArrayDeque<>();

        /** {@code true} if tasks must be blocked. */
        private volatile boolean block = true;

        /**
         * @param delegate Delegate executor.
         */
        public BlockingExecutor(Executor delegate) {
            this.delegate = delegate;
        }

        /** {@inheritDoc} */
        @Override public void execute(@NotNull Runnable cmd) {
            if (block)
                tasks.offer(cmd);
            else
                delegate.execute(cmd);
        }

        /** @param timeout Timeout in milliseconds. */
        public void waitForBlocked(long timeout) {
            try {
                assertTrue(waitForCondition(() -> !tasks.isEmpty(), timeout));
            }
            catch (IgniteInterruptedCheckedException e) {
                throw new IgniteException(e);
            }
        }

        /** Unblock and schedule tasks for execution. */
        public void unblock() {
            block = false;

            Runnable r;

            while ((r = tasks.poll()) != null) {
                delegate.execute(r);
            }
        }
    }
}
