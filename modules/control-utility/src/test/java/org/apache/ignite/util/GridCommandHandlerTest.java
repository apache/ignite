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

package org.apache.ignite.util;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.ShutdownPolicy;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridJobExecuteResponse;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.client.GridClientFactory;
import org.apache.ignite.internal.client.impl.GridClientImpl;
import org.apache.ignite.internal.client.util.GridConcurrentHashSet;
import org.apache.ignite.internal.commandline.CommandHandler;
import org.apache.ignite.internal.commandline.cache.argument.FindAndDeleteGarbageArg;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.processors.cache.ClusterStateTestUtils;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearLockResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointState;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.db.IgniteCacheGroupsWithRestartsTest;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.dumpprocessors.ToFileDumpProcessor;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.transactions.TransactionProxyImpl;
import org.apache.ignite.internal.processors.cache.verify.IdleVerifyResultV2;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cache.warmup.BlockedWarmUpConfiguration;
import org.apache.ignite.internal.processors.cache.warmup.BlockedWarmUpStrategy;
import org.apache.ignite.internal.processors.cache.warmup.WarmUpTestPluginProvider;
import org.apache.ignite.internal.processors.cluster.GridClusterStateProcessor;
import org.apache.ignite.internal.util.distributed.SingleNodeMessage;
import org.apache.ignite.internal.util.future.IgniteFinishedFutureImpl;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.lang.GridFunc;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.cache.VisorFindAndDeleteGarbageInPersistenceTaskResult;
import org.apache.ignite.internal.visor.tx.VisorTxInfo;
import org.apache.ignite.internal.visor.tx.VisorTxTaskResult;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionRollbackException;
import org.apache.ignite.transactions.TransactionTimeoutException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.io.File.separatorChar;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_CLUSTER_NAME;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.cache.PartitionLossPolicy.READ_ONLY_SAFE;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.ACTIVE_READ_ONLY;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.commandline.CommandHandler.CONFIRM_MSG;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_CONNECTION_FAILED;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_INVALID_ARGUMENTS;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_UNEXPECTED_ERROR;
import static org.apache.ignite.internal.commandline.CommandList.DEACTIVATE;
import static org.apache.ignite.internal.commandline.encryption.EncryptionSubcommands.CACHE_GROUP_KEY_IDS;
import static org.apache.ignite.internal.commandline.encryption.EncryptionSubcommands.CHANGE_CACHE_GROUP_KEY;
import static org.apache.ignite.internal.commandline.encryption.EncryptionSubcommands.REENCRYPTION_RATE;
import static org.apache.ignite.internal.commandline.encryption.EncryptionSubcommands.REENCRYPTION_RESUME;
import static org.apache.ignite.internal.commandline.encryption.EncryptionSubcommands.REENCRYPTION_STATUS;
import static org.apache.ignite.internal.commandline.encryption.EncryptionSubcommands.REENCRYPTION_SUSPEND;
import static org.apache.ignite.internal.encryption.AbstractEncryptionTest.MASTER_KEY_NAME_2;
import static org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager.IGNITE_PDS_SKIP_CHECKPOINT_ON_NODE_STOP;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.AbstractSnapshotSelfTest.doSnapshotCancellationTest;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.AbstractSnapshotSelfTest.snp;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.SNAPSHOT_METRICS;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.resolveSnapshotWorkDirectory;
import static org.apache.ignite.internal.processors.cache.verify.IdleVerifyUtility.GRID_NOT_IDLE_MSG;
import static org.apache.ignite.internal.processors.diagnostic.DiagnosticProcessor.DEFAULT_TARGET_FOLDER;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RESTORE_CACHE_GROUP_SNAPSHOT_PREPARE;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;

/**
 * Command line handler test.
 * You can use this class if you need create nodes for each test.
 * If you not necessary create nodes for each test you can try use {@link GridCommandHandlerClusterByClassTest}
 */
public class GridCommandHandlerTest extends GridCommandHandlerClusterPerMethodAbstractTest {
    /** Partitioned cache name. */
    protected static final String PARTITIONED_CACHE_NAME = "part_cache";

    /** Replicated cache name. */
    protected static final String REPLICATED_CACHE_NAME = "repl_cache";

    /** */
    protected static File defaultDiagnosticDir;

    /** */
    protected static File customDiagnosticDir;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        initDiagnosticDir();

        cleanDiagnosticDir();
    }

    /** {@inheritDoc} */
    @Override protected void cleanPersistenceDir() throws Exception {
        super.cleanPersistenceDir();

        cleanDiagnosticDir();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    protected void initDiagnosticDir() throws IgniteCheckedException {
        defaultDiagnosticDir = new File(U.defaultWorkDirectory()
            + separatorChar + DEFAULT_TARGET_FOLDER + separatorChar);

        customDiagnosticDir = new File(U.defaultWorkDirectory()
            + separatorChar + "diagnostic_test_dir" + separatorChar);
    }

    /**
     * Clean diagnostic directories.
     */
    protected void cleanDiagnosticDir() {
        U.delete(defaultDiagnosticDir);
        U.delete(customDiagnosticDir);
    }

    /**
     * Test activation works via control.sh
     *
     * @throws Exception If failed.
     */
    @Test
    public void testActivate() throws Exception {
        Ignite ignite = startGrids(1);

        injectTestSystemOut();

        assertEquals(INACTIVE, ignite.cluster().state());

        assertEquals(EXIT_CODE_OK, execute("--activate"));

        assertEquals(ACTIVE, ignite.cluster().state());

        assertContains(log, testOut.toString(), "Command deprecated. Use --set-state instead.");
    }

    /**
     * Test clients leakage.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testClientsLeakage() throws Exception {
        startGrids(1);

        Map<UUID, GridClientImpl> clnts = U.field(GridClientFactory.class, "openClients");

        Map<UUID, GridClientImpl> clntsBefore = new HashMap<>(clnts);

        assertEquals(EXIT_CODE_OK, execute("--set-state", "ACTIVE"));

        Map<UUID, GridClientImpl> clntsAfter1 = new HashMap<>(clnts);

        assertTrue("Still opened clients: " + new ArrayList<>(clnts.values()), clntsBefore.equals(clntsAfter1));

        stopAllGrids();

        assertEquals(EXIT_CODE_CONNECTION_FAILED, execute("--set-state", "ACTIVE"));

        Map<UUID, GridClientImpl> clntsAfter2 = new HashMap<>(clnts);

        assertTrue("Still opened clients: " + new ArrayList<>(clnts.values()), clntsBefore.equals(clntsAfter2));
    }

    private CacheConfiguration cacheConfiguration(String cacheName) {
        CacheConfiguration ccfg = new CacheConfiguration(cacheName)
            .setAtomicityMode(TRANSACTIONAL)
            .setAffinity(new RendezvousAffinityFunction(false, 32))
            .setBackups(1);

        return ccfg;
    }

    /**
     * Starts cluster of two nodes and prepares situation of corrupted PDS on node2
     * so it enters maintenance mode on restart.
     *
     * @param cachesToStart Configurations of caches that should be started in cluster.
     * @param cacheToCorrupt Function determining should cache with given name be corrupted or not.
     */
    private File startGridAndPutNodeToMaintenance(CacheConfiguration[] cachesToStart,
                                                  @Nullable Function<String, Boolean> cacheToCorrupt) throws Exception {
        assert cachesToStart != null && cachesToStart.length > 0;

        IgniteEx ig0 = startGrid(0);
        IgniteEx ig1 = startGrid(1);

        String ig1Folder = ig1.context().pdsFolderResolver().resolveFolders().folderName();
        File dbDir = U.resolveWorkDirectory(ig1.configuration().getWorkDirectory(), "db", false);

        File ig1LfsDir = new File(dbDir, ig1Folder);

        ig0.cluster().baselineAutoAdjustEnabled(false);
        ig0.cluster().state(ACTIVE);

        IgniteCache dfltCache = ig0.getOrCreateCache(cachesToStart[0]);

        if (cachesToStart.length > 1) {
            for (int i = 1; i < cachesToStart.length; i++)
                ig0.getOrCreateCache(cachesToStart[i]);
        }

        for (int k = 0; k < 1000; k++)
            dfltCache.put(k, k);

        GridCacheDatabaseSharedManager dbMrg0 = (GridCacheDatabaseSharedManager) ig0.context().cache().context().database();
        GridCacheDatabaseSharedManager dbMrg1 = (GridCacheDatabaseSharedManager) ig1.context().cache().context().database();

        dbMrg0.forceCheckpoint("cp").futureFor(CheckpointState.FINISHED).get();
        dbMrg1.forceCheckpoint("cp").futureFor(CheckpointState.FINISHED).get();

        Arrays.stream(cachesToStart)
            .map(ccfg -> ccfg.getName())
            .filter(name -> cacheToCorrupt.apply(name))
            .forEach(name -> ig0.cluster().disableWal(name));

        for (int k = 1000; k < 2000; k++)
            dfltCache.put(k, k);

        stopGrid(1);

        File[] cpMarkers = new File(ig1LfsDir, "cp").listFiles();

        for (File cpMark : cpMarkers) {
            if (cpMark.getName().contains("-END"))
                cpMark.delete();
        }

        assertThrows(log, () -> startGrid(1), Exception.class, null);

        return ig1LfsDir;
    }

    /**
     * Test verifies persistence clean command with explicit list of caches to be cleaned.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPersistenceCleanSpecifiedCachesCommand() throws Exception {
        String cacheName0 = DEFAULT_CACHE_NAME + "0";
        String cacheName1 = DEFAULT_CACHE_NAME + "1";
        String cacheName2 = DEFAULT_CACHE_NAME + "2";
        String cacheName3 = DEFAULT_CACHE_NAME + "3";

        String nonExistingCacheName = DEFAULT_CACHE_NAME + "4";

        File mntcNodeWorkDir = startGridAndPutNodeToMaintenance(
            new CacheConfiguration[]{
                cacheConfiguration(cacheName0),
                cacheConfiguration(cacheName1),
                cacheConfiguration(cacheName2),
                cacheConfiguration(cacheName3)
            },
            s -> !s.equals(cacheName3));

        IgniteEx ig1 = startGrid(1);

        String port = ig1.localNode().attribute(IgniteNodeAttributes.ATTR_REST_TCP_PORT).toString();

        assertEquals(EXIT_CODE_INVALID_ARGUMENTS, execute("--persistence", "clean", "caches",
            nonExistingCacheName,
            "--host", "localhost", "--port", port));

        assertEquals(EXIT_CODE_OK, execute("--persistence", "clean", "caches",
            cacheName0 + "," + cacheName1,
            "--host", "localhost", "--port", port));

        boolean cleanedEmpty = Arrays.stream(mntcNodeWorkDir.listFiles())
            .filter(f -> f.getName().contains(cacheName0) || f.getName().contains(cacheName1))
            .map(f -> f.listFiles().length == 1)
            .reduce(true, (t, u) -> t && u);

        assertTrue(cleanedEmpty);

        boolean nonCleanedNonEmpty = Arrays.stream(mntcNodeWorkDir.listFiles())
            .filter(f -> f.getName().contains(cacheName2) || f.getName().contains(cacheName3))
            .map(f -> f.listFiles().length > 1)
            .reduce(true, (t, u) -> t && u);

        assertTrue(nonCleanedNonEmpty);

        stopGrid(1);

        ig1 = startGrid(1);

        assertTrue(ig1.context().maintenanceRegistry().isMaintenanceMode());

        assertEquals(EXIT_CODE_OK, execute("--persistence", "clean", "caches",
            cacheName2,
            "--host", "localhost", "--port", port));

        stopGrid(1);

        ig1 = startGrid(1);

        assertFalse(ig1.context().maintenanceRegistry().isMaintenanceMode());
    }

    /**
     * Test verifies persistence clean command cleaning only corrupted caches and not touching others.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPersistenceCleanCorruptedCachesCommand() throws Exception {
        String cacheName0 = DEFAULT_CACHE_NAME + "0";
        String cacheName1 = DEFAULT_CACHE_NAME + "1";
        String cacheName2 = DEFAULT_CACHE_NAME + "2";
        String cacheName3 = DEFAULT_CACHE_NAME + "3";

        File mntcNodeWorkDir = startGridAndPutNodeToMaintenance(
            new CacheConfiguration[]{
                cacheConfiguration(cacheName0),
                cacheConfiguration(cacheName1),
                cacheConfiguration(cacheName2),
                cacheConfiguration(cacheName3)
            },
            s -> !s.equals(cacheName3));

        IgniteEx ig1 = startGrid(1);

        String port = ig1.localNode().attribute(IgniteNodeAttributes.ATTR_REST_TCP_PORT).toString();

        assertEquals(EXIT_CODE_OK, execute("--persistence", "clean", "corrupted",
            "--host", "localhost", "--port", port));

        boolean cleanedEmpty = Arrays.stream(mntcNodeWorkDir.listFiles())
            .filter(f ->
                f.getName().contains(cacheName0)
                || f.getName().contains(cacheName1)
                || f.getName().contains(cacheName2)
            )
            .map(f -> f.listFiles().length == 1)
            .reduce(true, (t, u) -> t && u);

        assertTrue(cleanedEmpty);

        stopGrid(1);

        ig1 = startGrid(1);

        assertFalse(ig1.context().maintenanceRegistry().isMaintenanceMode());
    }

    /**
     * Test verifies persistence clean all command that cleans all cache directories.
     *
     * @throws Exception
     */
    @Test
    public void testPersistenceCleanAllCachesCommand() throws Exception {
        String cacheName0 = DEFAULT_CACHE_NAME + "0";
        String cacheName1 = DEFAULT_CACHE_NAME + "1";

        File mntcNodeWorkDir = startGridAndPutNodeToMaintenance(
            new CacheConfiguration[]{
                cacheConfiguration(cacheName0),
                cacheConfiguration(cacheName1)
            },
            s -> s.equals(cacheName0));

        IgniteEx ig1 = startGrid(1);

        String port = ig1.localNode().attribute(IgniteNodeAttributes.ATTR_REST_TCP_PORT).toString();

        assertEquals(EXIT_CODE_OK, execute("--persistence", "clean", "all",
            "--host", "localhost", "--port", port));

        boolean allEmpty = Arrays.stream(mntcNodeWorkDir.listFiles())
            .filter(File::isDirectory)
            .filter(f -> f.getName().startsWith("cache-"))
            .map(f -> f.listFiles().length == 1)
            .reduce(true, (t, u) -> t && u);

        assertTrue(allEmpty);

        stopGrid(1);

        ig1 = startGrid(1);

        assertFalse(ig1.context().maintenanceRegistry().isMaintenanceMode());
    }

    /**
     * Test verifies that persistence backup command to backup all caches backs up all cache directories.
     * 
     * @throws Exception If failed.
     */
    @Test
    public void testPersistenceBackupAllCachesCommand() throws Exception {
        String cacheName0 = DEFAULT_CACHE_NAME + "0";
        String cacheName1 = DEFAULT_CACHE_NAME + "1";

        File mntcNodeWorkDir = startGridAndPutNodeToMaintenance(
            new CacheConfiguration[]{
                cacheConfiguration(cacheName0),
                cacheConfiguration(cacheName1)
            },
            s -> s.equals(cacheName0));

        IgniteEx ig1 = startGrid(1);

        String port = ig1.localNode().attribute(IgniteNodeAttributes.ATTR_REST_TCP_PORT).toString();

        assertEquals(EXIT_CODE_OK, execute("--persistence", "backup", "all",
            "--host", "localhost", "--port", port));

        Set<String> backedUpCacheDirs = Arrays.stream(mntcNodeWorkDir.listFiles())
            .filter(File::isDirectory)
            .filter(f -> f.getName().startsWith("backup_"))
            .map(f -> f.getName().substring("backup_".length()))
            .collect(Collectors.toCollection(TreeSet::new));

        Set<String> allCacheDirs = Arrays.stream(mntcNodeWorkDir.listFiles())
            .filter(File::isDirectory)
            .filter(f -> f.getName().startsWith("cache-"))
            .map(File::getName)
            .collect(Collectors.toCollection(TreeSet::new));

        assertEqualsCollections(backedUpCacheDirs, allCacheDirs);

        checkCacheAndBackupDirsContent(mntcNodeWorkDir);
    }

    /**
     * Test verifies that persistence backup command copies all corrupted caches content to backup directory
     * but does not touch other directories.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPersistenceBackupCorruptedCachesCommand() throws Exception {
        String cacheName0 = DEFAULT_CACHE_NAME + "0";
        String cacheName1 = DEFAULT_CACHE_NAME + "1";

        File mntcNodeWorkDir = startGridAndPutNodeToMaintenance(
            new CacheConfiguration[]{
                cacheConfiguration(cacheName0),
                cacheConfiguration(cacheName1)
            },
            s -> s.equals(cacheName0));

        IgniteEx ig1 = startGrid(1);

        String port = ig1.localNode().attribute(IgniteNodeAttributes.ATTR_REST_TCP_PORT).toString();

        assertEquals(EXIT_CODE_OK, execute("--persistence", "backup", "corrupted",
            "--host", "localhost", "--port", port));

        long backedUpCachesCnt = Arrays.stream(mntcNodeWorkDir.listFiles())
            .filter(File::isDirectory)
            .filter(f -> f.getName().startsWith("backup_"))
            .filter(f -> f.getName().contains(cacheName0))
            .count();

        assertEquals(1, backedUpCachesCnt);

        checkCacheAndBackupDirsContent(mntcNodeWorkDir);
    }

    /**
     * Test verifies that persistence backup command with specified caches copied only content of that caches and
     * doesn't touch other directories.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPersistenceBackupSpecifiedCachesCommand() throws Exception {
        String cacheName0 = DEFAULT_CACHE_NAME + "0";
        String cacheName1 = DEFAULT_CACHE_NAME + "1";
        String cacheName2 = DEFAULT_CACHE_NAME + "2";

        String nonExistingCacheName = "nonExistingCache";

        File mntcNodeWorkDir = startGridAndPutNodeToMaintenance(
            new CacheConfiguration[]{
                cacheConfiguration(cacheName0),
                cacheConfiguration(cacheName1),
                cacheConfiguration(cacheName2)
            },
            s -> s.equals(cacheName0) || s.equals(cacheName2));

        IgniteEx ig1 = startGrid(1);

        String port = ig1.localNode().attribute(IgniteNodeAttributes.ATTR_REST_TCP_PORT).toString();

        assertEquals(EXIT_CODE_INVALID_ARGUMENTS, execute("--persistence", "backup", "caches",
            nonExistingCacheName,
            "--host", "localhost", "--port", port));

        assertEquals(EXIT_CODE_OK, execute("--persistence", "backup", "caches",
            cacheName0 + "," + cacheName2,
            "--host", "localhost", "--port", port));

        long backedUpCachesCnt = Arrays.stream(mntcNodeWorkDir.listFiles())
            .filter(File::isDirectory)
            .filter(f -> f.getName().startsWith("backup_"))
            .count();

        assertEquals(2, backedUpCachesCnt);

        checkCacheAndBackupDirsContent(mntcNodeWorkDir);
    }

    /** */
    private void checkCacheAndBackupDirsContent(File mntcNodeWorkDir) {
        List<File> backupDirs = Arrays.stream(mntcNodeWorkDir.listFiles())
            .filter(File::isDirectory)
            .filter(f -> f.getName().startsWith("backup_"))
            .collect(Collectors.toList());

        Path mntcNodeWorkDirPath = mntcNodeWorkDir.toPath();

        for (File bDir : backupDirs) {
            File origCacheDir = mntcNodeWorkDirPath.resolve(bDir.getName().substring("backup_".length())).toFile();

            assertTrue(origCacheDir.isDirectory());

            assertEquals(origCacheDir.listFiles().length, bDir.listFiles().length);
        }
    }

    /**
     * Test enabling/disabling read-only mode works via control.sh
     *
     * @throws Exception If failed.
     */
    @Test
    public void testReadOnlyEnableDisable() throws Exception {
        Ignite ignite = startGrids(1);

        ignite.cluster().state(ACTIVE);

        assertEquals(ACTIVE, ignite.cluster().state());

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--set-state", "ACTIVE_READ_ONLY"));

        assertEquals(ACTIVE_READ_ONLY, ignite.cluster().state());

        assertContains(log, testOut.toString(), "Cluster state changed to ACTIVE_READ_ONLY");

        assertEquals(EXIT_CODE_OK, execute("--set-state", "ACTIVE"));

        assertEquals(ACTIVE, ignite.cluster().state());

        assertContains(log, testOut.toString(), "Cluster state changed to ACTIVE");
    }

    /**
     * Verifies that update-tag action obeys its specification: doesn't allow updating tag on inactive cluster,
     *
     * @throws Exception If failed.
     */
    @Test
    public void testClusterChangeTag() throws Exception {
        final String newTag = "new_tag";

        IgniteEx cl = startGrid(0);

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--change-tag", newTag));

        String out = testOut.toString();

        //because cluster is inactive
        assertTrue(out.contains("Error has occurred during tag update:"));

        cl.cluster().active(true);

        //because new tag should be non-empty string
        assertEquals(EXIT_CODE_INVALID_ARGUMENTS, execute("--change-tag", ""));

        assertEquals(EXIT_CODE_OK, execute("--change-tag", newTag));

        boolean tagUpdated = GridTestUtils.waitForCondition(() -> newTag.equals(cl.cluster().tag()), 10_000);
        assertTrue("Tag has not been updated in 10 seconds", tagUpdated);
    }

    /**
     * Test deactivation works via control.sh
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDeactivate() throws Exception {
        Ignite ignite = startGrids(1);

        assertFalse(ignite.cluster().active());
        assertEquals(INACTIVE, ignite.cluster().state());

        ignite.cluster().state(ACTIVE);

        assertTrue(ignite.cluster().active());
        assertEquals(ACTIVE, ignite.cluster().state());

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--deactivate"));

        assertFalse(ignite.cluster().active());
        assertEquals(INACTIVE, ignite.cluster().state());

        assertContains(log, testOut.toString(), "Command deprecated. Use --set-state instead.");
    }

    /**
     * Test "deactivate" via control.sh when a non-persistent cache involved.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDeactivateNonPersistent() throws Exception {
        checkDeactivateNonPersistent("--deactivate");
    }

    /**
     * Test "set-state inactive" via control.sh when a non-persistent cache involved.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSetInactiveNonPersistent() throws Exception {
        checkDeactivateNonPersistent("--set-state", "inactive");
    }

    /**
     * Launches cluster deactivation. Works via control.sh when a non-persistent cache involved.
     *
     *  @param cmd Certain command to deactivate cluster.
     */
    private void checkDeactivateNonPersistent(String... cmd) throws Exception {
        dataRegionConfiguration = new DataRegionConfiguration()
            .setName("non-persistent-dataRegion")
            .setPersistenceEnabled(false);

        Ignite ignite = startGrids(1);

        ignite.cluster().state(ACTIVE);

        assertTrue(ignite.cluster().active());
        assertEquals(ACTIVE, ignite.cluster().state());

        ignite.createCache(new CacheConfiguration<>("non-persistent-cache")
            .setDataRegionName("non-persistent-dataRegion"));

        injectTestSystemOut();

        assertEquals(EXIT_CODE_UNEXPECTED_ERROR, execute(cmd));

        assertTrue(ignite.cluster().active());
        assertEquals(ACTIVE, ignite.cluster().state());
        assertContains(log, testOut.toString(), GridClusterStateProcessor.DATA_LOST_ON_DEACTIVATION_WARNING);

        List<String> forceCmd = new ArrayList<>(Arrays.asList(cmd));
        forceCmd.add("--force");

        assertEquals(EXIT_CODE_OK, execute(forceCmd));

        assertFalse(ignite.cluster().active());
        assertEquals(INACTIVE, ignite.cluster().state());
    }

    /**
     * Test the deactivation command on the active and no cluster with checking
     * the cluster name(which is set through the system property) in
     * confirmation.
     *
     * @throws Exception If failed.
     * */
    @Test
    @WithSystemProperty(key = IGNITE_CLUSTER_NAME, value = "TEST_CLUSTER_NAME")
    public void testDeactivateWithCheckClusterNameInConfirmationBySystemProperty() throws Exception {
        IgniteEx igniteEx = startGrid(0);
        assertFalse(igniteEx.cluster().active());

        deactivateActiveOrNotClusterWithCheckClusterNameInConfirmation(igniteEx, "TEST_CLUSTER_NAME");
    }

    /**
     * Test the deactivation command on the active and no cluster with checking
     * the cluster name(default) in confirmation.
     *
     * @throws Exception If failed.
     * */
    @Test
    public void testDeactivateWithCheckClusterNameInConfirmationByDefault() throws Exception {
        IgniteEx igniteEx = startGrid(0);
        assertFalse(igniteEx.cluster().active());

        deactivateActiveOrNotClusterWithCheckClusterNameInConfirmation(
            igniteEx,
            igniteEx.context().cache().utilityCache().context().dynamicDeploymentId().toString()
        );
    }

    /**
     * Deactivating the cluster(active and not) with checking the cluster name
     * in the confirmation.
     *
     * @param igniteEx Node.
     * @param clusterName Cluster name to check in the confirmation message.
     * */
    private void deactivateActiveOrNotClusterWithCheckClusterNameInConfirmation(
        IgniteEx igniteEx,
        String clusterName
    ) {
        deactivateWithCheckClusterNameInConfirmation(igniteEx, clusterName);

        igniteEx.cluster().active(true);
        assertTrue(igniteEx.cluster().active());

        deactivateWithCheckClusterNameInConfirmation(igniteEx, clusterName);
    }

    /**
     * Deactivating the cluster with checking the cluster name in the
     * confirmation.
     *
     * @param igniteEx Node.
     * @param clusterName Cluster name to check in the confirmation message.
     * */
    private void deactivateWithCheckClusterNameInConfirmation(IgniteEx igniteEx, String clusterName) {
        autoConfirmation = false;
        injectTestSystemOut();
        injectTestSystemIn(CONFIRM_MSG);

        assertEquals(EXIT_CODE_OK, execute(DEACTIVATE.text()));
        assertFalse(igniteEx.cluster().active());

        assertContains(
            log,
            testOut.toString(),
            "Warning: the command will deactivate a cluster \"" + clusterName + "\"."
        );
    }

    /**
     * Test cluster active state works via control.sh
     *
     * @throws Exception If failed.
     */
    @Test
    public void testState() throws Exception {
        final String newTag = "new_tag";

        Ignite ignite = startGrids(1);

        assertFalse(ignite.cluster().active());

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--state"));

        assertContains(log, testOut.toString(), "Cluster is inactive");

        String out = testOut.toString();

        UUID clId = ignite.cluster().id();
        String clTag = ignite.cluster().tag();

        assertTrue(out.contains("Cluster  ID: " + clId));
        assertTrue(out.contains("Cluster tag: " + clTag));

        ignite.cluster().active(true);

        assertTrue(ignite.cluster().active());

        assertEquals(EXIT_CODE_OK, execute("--state"));

        assertContains(log, testOut.toString(), "Cluster is active");

        ignite.cluster().state(ACTIVE_READ_ONLY);

        awaitPartitionMapExchange();

        assertEquals(ACTIVE_READ_ONLY, ignite.cluster().state());

        assertEquals(EXIT_CODE_OK, execute("--state"));

        assertContains(log, testOut.toString(), "Cluster is active (read-only)");

        boolean tagUpdated = GridTestUtils.waitForCondition(() -> {
            try {
                ignite.cluster().tag(newTag);
            }
            catch (IgniteCheckedException e) {
                return false;
            }

            return true;
        }, 10_000);

        assertTrue("Tag has not been updated in 10 seconds.", tagUpdated);

        assertEquals(EXIT_CODE_OK, execute("--state"));

        out = testOut.toString();

        assertTrue(out.contains("Cluster tag: " + newTag));
    }

    /**
     * Test --set-state command works correct.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSetState() throws Exception {
        Ignite ignite = startGrids(2);

        ignite.cluster().state(ACTIVE);

        ignite.createCache(ClusterStateTestUtils.partitionedCache(PARTITIONED_CACHE_NAME));
        ignite.createCache(ClusterStateTestUtils.replicatedCache(REPLICATED_CACHE_NAME));

        ignite.cluster().state(INACTIVE);

        injectTestSystemOut();

        assertEquals(INACTIVE, ignite.cluster().state());

        // INACTIVE -> INACTIVE.
        setState(ignite, INACTIVE, "INACTIVE", PARTITIONED_CACHE_NAME, REPLICATED_CACHE_NAME);

        // INACTIVE -> ACTIVE_READ_ONLY.
        setState(ignite, ACTIVE_READ_ONLY, "ACTIVE_READ_ONLY", PARTITIONED_CACHE_NAME, REPLICATED_CACHE_NAME);

        // ACTIVE_READ_ONLY -> ACTIVE_READ_ONLY.
        setState(ignite, ACTIVE_READ_ONLY, "ACTIVE_READ_ONLY", PARTITIONED_CACHE_NAME, REPLICATED_CACHE_NAME);

        // ACTIVE_READ_ONLY -> ACTIVE.
        setState(ignite, ACTIVE, "ACTIVE", PARTITIONED_CACHE_NAME, REPLICATED_CACHE_NAME);

        // ACTIVE -> ACTIVE.
        setState(ignite, ACTIVE, "ACTIVE", PARTITIONED_CACHE_NAME, REPLICATED_CACHE_NAME);

        // ACTIVE -> INACTIVE.
        setState(ignite, INACTIVE, "INACTIVE", PARTITIONED_CACHE_NAME, REPLICATED_CACHE_NAME);

        // INACTIVE -> ACTIVE.
        setState(ignite, ACTIVE, "ACTIVE", PARTITIONED_CACHE_NAME, REPLICATED_CACHE_NAME);

        // ACTIVE -> ACTIVE_READ_ONLY.
        setState(ignite, ACTIVE_READ_ONLY, "ACTIVE_READ_ONLY", PARTITIONED_CACHE_NAME, REPLICATED_CACHE_NAME);

        // ACTIVE_READ_ONLY -> INACTIVE.
        setState(ignite, INACTIVE, "INACTIVE", PARTITIONED_CACHE_NAME, REPLICATED_CACHE_NAME);
    }

    /** */
    private void setState(Ignite ignite, ClusterState state, String strState, String... cacheNames) {
        log.info(ignite.cluster().state() + " -> " + state);

        assertEquals(EXIT_CODE_OK, execute("--set-state", strState));

        assertEquals(state, ignite.cluster().state());

        assertContains(log, testOut.toString(), "Cluster state changed to " + strState);

        List<IgniteEx> nodes = IntStream.range(0, 2)
            .mapToObj(this::grid)
            .collect(Collectors.toList());

        ClusterStateTestUtils.putSomeDataAndCheck(log, nodes, cacheNames);

        if (state == ACTIVE) {
            for (String cacheName : cacheNames)
                grid(0).cache(cacheName).clear();
        }
    }

    /**
     * Test baseline collect works via control.sh
     *
     * @throws Exception If failed.
     */
    @Test
    public void testBaselineCollect() throws Exception {
        Ignite ignite = startGrid(
            optimize(getConfiguration(getTestIgniteInstanceName(0))).setLocalHost("0.0.0.0"));

        Field addresses = ignite.cluster().node().getClass().getDeclaredField("addrs");
        addresses.setAccessible(true);
        addresses.set(ignite.cluster().node(), Arrays.asList("127.0.0.1", "0:0:0:0:0:0:0:1", "10.19.112.175", "188.166.164.247"));
        Field hostNames = ignite.cluster().node().getClass().getDeclaredField("hostNames");
        hostNames.setAccessible(true);
        hostNames.set(ignite.cluster().node(), Arrays.asList("10.19.112.175.hostname"));

        assertFalse(ignite.cluster().active());

        ignite.cluster().active(true);

        injectTestSystemOut();

        { // non verbose mode
            assertEquals(EXIT_CODE_OK, execute("--baseline"));

            List<String> nodesInfo = findBaselineNodesInfo();
            assertEquals(1, nodesInfo.size());
            assertContains(log, nodesInfo.get(0), "Address=188.166.164.247.hostname/188.166.164.247, ");
        }

        { // verbose mode
            assertEquals(EXIT_CODE_OK, execute("--verbose", "--baseline"));

            List<String> nodesInfo = findBaselineNodesInfo();
            assertEquals(1, nodesInfo.size());
            assertContains(
                log,
                nodesInfo.get(0),
                "Addresses=188.166.164.247.hostname/188.166.164.247,10.19.112.175.hostname/10.19.112.175"
            );
        }

        { // empty resolved addresses
            addresses.set(ignite.cluster().node(), Collections.emptyList());
            hostNames.set(ignite.cluster().node(), Collections.emptyList());

            assertEquals(EXIT_CODE_OK, execute("--verbose", "--baseline"));

            List<String> nodesInfo = findBaselineNodesInfo();
            assertEquals(1, nodesInfo.size());
            assertContains(log, nodesInfo.get(0), "ConsistentId=" +
                grid(0).cluster().localNode().consistentId() + ", State=");
        }

        assertEquals(1, ignite.cluster().currentBaselineTopology().size());
    }

    /**
     * Test baseline collect works via control.sh
     *
     * @throws Exception If failed.
     */
    @Test
    public void testBaselineCollectCrd() throws Exception {
        Ignite ignite = startGrids(2);

        assertFalse(ignite.cluster().active());

        ignite.cluster().active(true);

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--baseline", "--port", "11212"));

        String crdStr = findCrdInfo();

        assertEquals("(Coordinator: ConsistentId=" +
            grid(0).cluster().localNode().consistentId() + ", Address=127.0.0.1.hostname/127.0.0.1" + ", Order=1)", crdStr);

        stopGrid(0);

        assertEquals(EXIT_CODE_OK, execute("--baseline", "--port", "11212"));

        crdStr = findCrdInfo();

        assertEquals("(Coordinator: ConsistentId=" +
            grid(1).cluster().localNode().consistentId() + ", Address=127.0.0.1.hostname/127.0.0.1" + ", Order=2)", crdStr);

        startGrid(0);

        assertEquals(EXIT_CODE_OK, execute("--baseline", "--port", "11212"));

        crdStr = findCrdInfo();

        assertEquals("(Coordinator: ConsistentId=" +
            grid(1).cluster().localNode().consistentId() + ", Address=127.0.0.1.hostname/127.0.0.1" + ", Order=2)", crdStr);

        stopGrid(1);

        assertEquals(EXIT_CODE_OK, execute("--baseline", "--port", "11211"));

        crdStr = findCrdInfo();

        assertEquals("(Coordinator: ConsistentId=" +
            grid(0).cluster().localNode().consistentId() + ", Address=127.0.0.1.hostname/127.0.0.1" + ", Order=4)", crdStr);
    }

    /**
     * @return utility information about coordinator
     */
    private String findCrdInfo() {
        String outStr = testOut.toString();

        int i = outStr.indexOf("(Coordinator: ConsistentId=");

        assertTrue(i != -1);

        String crdStr = outStr.substring(i).trim();

        return crdStr.substring(0, crdStr.indexOf('\n')).trim();
    }

    /**
     * @return utility information about baseline nodes
     */
    private List<String> findBaselineNodesInfo() {
        String outStr = testOut.toString();

        int i = outStr.indexOf("Baseline nodes:");

        assertTrue("Baseline nodes information is not found", i != -1);

        int j = outStr.indexOf("\n", i) + 1;

        int beginOfNodeDesc = -1;

        List<String> nodesInfo = new ArrayList<>();

        while ((beginOfNodeDesc = outStr.indexOf("ConsistentId=", j) ) != -1) {
            j = outStr.indexOf("\n", beginOfNodeDesc);
            nodesInfo.add(outStr.substring(beginOfNodeDesc, j).trim());
        }

        return nodesInfo;
    }

    /**
     * @param ignites Ignites.
     * @return Local node consistent ID.
     */
    private String consistentIds(Ignite... ignites) {
        StringBuilder res = new StringBuilder();

        for (Ignite ignite : ignites) {
            String consistentId = ignite.cluster().localNode().consistentId().toString();

            if (res.length() != 0)
                res.append(", ");

            res.append(consistentId);
        }

        return res.toString();
    }

    /**
     * Test baseline add items works via control.sh
     *
     * @throws Exception If failed.
     */
    @Test
    public void testBaselineAdd() throws Exception {
        Ignite ignite = startGrids(1);

        ignite.cluster().baselineAutoAdjustEnabled(false);

        assertFalse(ignite.cluster().active());

        ignite.cluster().active(true);

        assertEquals(EXIT_CODE_INVALID_ARGUMENTS, execute("--baseline", "add"));

        assertEquals(EXIT_CODE_INVALID_ARGUMENTS, execute("--baseline", "add", "non-existent-id"));

        Ignite other = startGrid(2);

        assertEquals(EXIT_CODE_OK, execute("--baseline", "add", consistentIds(other)));

        assertEquals(2, ignite.cluster().currentBaselineTopology().size());
    }

    /**
     * Test connectivity command works via control.sh.
     */
    @Test
    public void testConnectivityCommandWithoutFailedNodes() throws Exception {
        IgniteEx ignite = startGrids(5);

        assertFalse(ignite.cluster().state().active());

        ignite.cluster().state(ACTIVE);

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--diagnostic", "connectivity"));

        assertContains(log, testOut.toString(), "There are no connectivity problems.");
    }

    /**
     * Test that if node exits topology during connectivity check, the command will not fail.
     *
     * Description:
     * 1. Start three nodes.
     * 2. Execute connectivity check.
     * 3. When 3-rd node receives connectivity check compute task, it must stop itself.
     * 4. The command should exit with code OK.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testConnectivityCommandWithNodeExit() throws Exception {
        IgniteEx[] node3 = new IgniteEx[1];

        class KillNode3CommunicationSpi extends TcpCommunicationSpi {
            /** Fail check connection request and stop third node */
            boolean fail;

            public KillNode3CommunicationSpi(boolean fail) {
                this.fail = fail;
            }

            /** {@inheritDoc} */
            @Override public IgniteFuture<BitSet> checkConnection(List<ClusterNode> nodes) {
                if (fail) {
                    runAsync(node3[0]::close);
                    return null;
                }

                return super.checkConnection(nodes);
            }
        }

        IgniteEx node1 = startGrid(1, (UnaryOperator<IgniteConfiguration>) configuration -> {
            configuration.setCommunicationSpi(new KillNode3CommunicationSpi(false));
            return configuration;
        });

        IgniteEx node2 = startGrid(2, (UnaryOperator<IgniteConfiguration>) configuration -> {
            configuration.setCommunicationSpi(new KillNode3CommunicationSpi(false));
            return configuration;
        });

        node3[0] = startGrid(3, (UnaryOperator<IgniteConfiguration>) configuration -> {
            configuration.setCommunicationSpi(new KillNode3CommunicationSpi(true));
            return configuration;
        });

        assertFalse(node1.cluster().state().active());

        node1.cluster().state(ACTIVE);

        assertEquals(3, node1.cluster().nodes().size());

        injectTestSystemOut();

        final IgniteInternalFuture<?> connectivity = runAsync(() -> {
            final int result = execute("--diagnostic", "connectivity");
            assertEquals(EXIT_CODE_OK, result);
        });

        connectivity.get();
    }

    /**
     * Test connectivity command works via control.sh with one node failing.
     */
    @Test
    public void testConnectivityCommandWithFailedNodes() throws Exception {
        UUID okId = UUID.randomUUID();
        UUID failingId = UUID.randomUUID();

        UnaryOperator<IgniteConfiguration> operator = configuration -> {
            configuration.setCommunicationSpi(new TcpCommunicationSpi() {
                /** {inheritDoc} */
                @Override public IgniteFuture<BitSet> checkConnection(List<ClusterNode> nodes) {
                    BitSet bitSet = new BitSet();

                    int idx = 0;

                    for (ClusterNode remoteNode : nodes) {
                        if (!remoteNode.id().equals(failingId))
                            bitSet.set(idx);

                        idx++;
                    }

                    return new IgniteFinishedFutureImpl<>(bitSet);
                }
            });
            return configuration;
        };

        IgniteEx ignite = startGrid("normal", configuration -> {
            operator.apply(configuration);
            configuration.setConsistentId(okId);
            configuration.setNodeId(okId);
            return configuration;
        });

        IgniteEx failure = startGrid("failure", configuration -> {
            operator.apply(configuration);
            configuration.setConsistentId(failingId);
            configuration.setNodeId(failingId);
            return configuration;
        });

        ignite.cluster().state(ACTIVE);

        failure.cluster().state(ACTIVE);

        injectTestSystemOut();

        int connectivity = execute("--diagnostic", "connectivity");
        assertEquals(EXIT_CODE_OK, connectivity);

        String out = testOut.toString();
        String what = "There is no connectivity between the following nodes";

        assertContains(log, out.replaceAll("[\\W_]+", "").trim(),
                            what.replaceAll("[\\W_]+", "").trim());
    }

    /**
     * Test baseline remove works via control.sh
     *
     * @throws Exception If failed.
     */
    @Test
    public void testBaselineRemove() throws Exception {
        Ignite ignite = startGrids(1);

        ignite.cluster().baselineAutoAdjustEnabled(false);

        Ignite other = startGrid("nodeToStop");

        assertFalse(ignite.cluster().active());

        ignite.cluster().active(true);

        String offlineNodeConsId = consistentIds(other);

        stopGrid("nodeToStop");

        assertEquals(EXIT_CODE_OK, execute("--baseline"));
        assertEquals(EXIT_CODE_OK, execute("--baseline", "remove", offlineNodeConsId));

        assertEquals(1, ignite.cluster().currentBaselineTopology().size());
    }

    /**
     * Test is checking how to --shutdown-policy command works through control.sh.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testShutdownPolicy() throws Exception {
        Ignite ignite = startGrids(1);

        assertFalse(ignite.cluster().active());

        ignite.cluster().active(true);

        ShutdownPolicy policy = ignite.cluster().shutdownPolicy();

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--shutdown-policy"));

        String out = testOut.toString();

        assertContains(log, out, "Cluster shutdown policy is " + policy);
    }

    /**
     * Change shutdown policy through command.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testShutdownPolicyChange() throws Exception {
        Ignite ignite = startGrids(1);

        assertFalse(ignite.cluster().active());

        ignite.cluster().active(true);

        ShutdownPolicy policyToChange = null;

        for (ShutdownPolicy policy : ShutdownPolicy.values()) {
            if (policy != ignite.cluster().shutdownPolicy())
                policyToChange = policy;
        }

        assertNotNull(policyToChange);

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--shutdown-policy", policyToChange.name()));

        assertSame(policyToChange, ignite.cluster().shutdownPolicy());

        String out = testOut.toString();

        assertContains(log, out, "Cluster shutdown policy is " + policyToChange);
    }

    /**
     * Test baseline remove node on not active cluster via control.sh
     *
     * @throws Exception If failed.
     */
    @Test
    public void testBaselineRemoveOnNotActiveCluster() throws Exception {
        Ignite ignite = startGrids(1);
        Ignite other = startGrid("nodeToStop");

        assertFalse(ignite.cluster().active());

        String offlineNodeConsId = consistentIds(other);

        assertEquals(EXIT_CODE_UNEXPECTED_ERROR, execute("--baseline", "remove", offlineNodeConsId));

        ignite.cluster().active(true);

        stopGrid("nodeToStop");

        assertEquals(2, ignite.cluster().currentBaselineTopology().size());

        ignite.cluster().active(false);

        assertFalse(ignite.cluster().active());

        injectTestSystemOut();

        assertEquals(EXIT_CODE_UNEXPECTED_ERROR, execute("--baseline", "remove", offlineNodeConsId));

        assertContains(log, testOut.toString(), "Changing BaselineTopology on inactive cluster is not allowed.");
    }

    /**
     * Test baseline set works via control.sh
     *
     * @throws Exception If failed.
     */
    @Test
    public void testBaselineSet() throws Exception {
        Ignite ignite = startGrids(1);

        ignite.cluster().baselineAutoAdjustEnabled(false);

        assertFalse(ignite.cluster().active());

        ignite.cluster().active(true);

        Ignite other = startGrid(2);

        assertEquals(EXIT_CODE_OK, execute("--baseline", "set", consistentIds(ignite, other)));

        assertEquals(2, ignite.cluster().currentBaselineTopology().size());

        assertEquals(EXIT_CODE_INVALID_ARGUMENTS, execute("--baseline", "set", "invalidConsistentId"));
    }

    /**
     * Test baseline set nodes with baseline offline node works via control.sh
     *
     * @throws Exception If failed.
     */
    @Test
    public void testBaselineSetWithOfflineNode() throws Exception {
        Ignite ignite0 = startGrid(0);
        //It is important to set consistent id to null for force autogeneration.
        Ignite ignite1 = startGrid(optimize(getConfiguration(getTestIgniteInstanceName(1)).setConsistentId(null)));

        assertFalse(ignite0.cluster().active());

        ignite0.cluster().active(true);

        Ignite other = startGrid(2);

        String consistentIds = consistentIds(ignite0, ignite1, other);

        ignite1.close();

        assertEquals(EXIT_CODE_OK, execute("--baseline", "set", consistentIds));

        assertEquals(3, ignite0.cluster().currentBaselineTopology().size());
    }

    /**
     * Test baseline set by topology version works via control.sh
     *
     * @throws Exception If failed.
     */
    @Test
    public void testBaselineVersion() throws Exception {
        Ignite ignite = startGrids(1);

        ignite.cluster().baselineAutoAdjustEnabled(false);

        assertFalse(ignite.cluster().active());

        ignite.cluster().active(true);

        startGrid(2);

        assertEquals(EXIT_CODE_OK, execute("--baseline"));

        assertEquals(EXIT_CODE_OK, execute("--baseline", "version", String.valueOf(ignite.cluster().topologyVersion())));

        assertEquals(2, ignite.cluster().currentBaselineTopology().size());
    }

    /**
     * Test that updating of baseline auto_adjustment settings via control.sh actually influence cluster's baseline.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testBaselineAutoAdjustmentAutoRemoveNode() throws Exception {
        Ignite ignite = startGrids(3);

        ignite.cluster().active(true);

        int timeout = 2000;

        assertEquals(EXIT_CODE_OK,
            execute("--baseline", "auto_adjust", "enable", "timeout", String.valueOf(timeout)));

        assertEquals(3, ignite.cluster().currentBaselineTopology().size());

        CountDownLatch nodeLeftLatch = new CountDownLatch(1);

        AtomicLong nodeLeftTime = new AtomicLong();

        ignite.events().localListen(event -> {
            nodeLeftTime.set(event.timestamp());

            nodeLeftLatch.countDown();

            return false;
        }, EVT_NODE_LEFT, EVT_NODE_FAILED);

        runAsync(() -> stopGrid(2));

        nodeLeftLatch.await();

        while (true) {
            int bltSize = ignite.cluster().currentBaselineTopology().size();

            if (System.currentTimeMillis() >= nodeLeftTime.get() + timeout)
                break;

            assertEquals(3, bltSize);

            U.sleep(100);
        }

        assertTrue(waitForCondition(() -> ignite.cluster().currentBaselineTopology().size() == 2, 10000));

        Collection<BaselineNode> baselineNodesAfter = ignite.cluster().currentBaselineTopology();

        assertEquals(EXIT_CODE_OK, execute("--baseline", "auto_adjust", "disable"));

        stopGrid(1);

        Thread.sleep(3000L);

        Collection<BaselineNode> baselineNodesFinal = ignite.cluster().currentBaselineTopology();

        assertEquals(
            baselineNodesAfter.stream().map(BaselineNode::consistentId).collect(Collectors.toList()),
            baselineNodesFinal.stream().map(BaselineNode::consistentId).collect(Collectors.toList())
        );
    }

    /**
     * Test that updating of baseline auto_adjustment settings via control.sh actually influence cluster's baseline.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testBaselineAutoAdjustmentAutoAddNode() throws Exception {
        Ignite ignite = startGrids(1);

        ignite.cluster().active(true);

        assertEquals(EXIT_CODE_OK, execute("--baseline", "auto_adjust", "enable", "timeout", "2000"));

        assertEquals(1, ignite.cluster().currentBaselineTopology().size());

        startGrid(1);

        assertEquals(1, ignite.cluster().currentBaselineTopology().size());

        assertEquals(EXIT_CODE_OK, execute("--baseline"));

        assertTrue(waitForCondition(() -> ignite.cluster().currentBaselineTopology().size() == 2, 10000));

        Collection<BaselineNode> baselineNodesAfter = ignite.cluster().currentBaselineTopology();

        assertEquals(EXIT_CODE_OK, execute("--baseline", "auto_adjust", "disable"));

        startGrid(2);

        Thread.sleep(3000L);

        Collection<BaselineNode> baselineNodesFinal = ignite.cluster().currentBaselineTopology();

        assertEquals(
            baselineNodesAfter.stream().map(BaselineNode::consistentId).collect(Collectors.toList()),
            baselineNodesFinal.stream().map(BaselineNode::consistentId).collect(Collectors.toList())
        );
    }

    /**
     * Test active transactions.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testActiveTransactions() throws Exception {
        Ignite ignite = startGridsMultiThreaded(2);

        ignite.cluster().active(true);

        Ignite client = startGrid("client");

        client.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(TRANSACTIONAL).setWriteSynchronizationMode(FULL_SYNC));

        for (Ignite ig : G.allGrids())
            assertNotNull(ig.cache(DEFAULT_CACHE_NAME));

        CountDownLatch lockLatch = new CountDownLatch(1);
        CountDownLatch unlockLatch = new CountDownLatch(1);

        IgniteInternalFuture<?> fut = startTransactions("testActiveTransactions", lockLatch, unlockLatch, true);

        U.awaitQuiet(lockLatch);

        doSleep(5000);

        CommandHandler h = new CommandHandler();

        final VisorTxInfo[] toKill = {null};

        // Basic test.
        validate(h, map -> {
            VisorTxTaskResult res = map.get(grid(0).cluster().localNode());

            for (VisorTxInfo info : res.getInfos()) {
                if (info.getSize() == 100) {
                    toKill[0] = info; // Store for further use.

                    break;
                }
            }

            assertEquals(3, map.size());
        }, "--tx");

        assertNotNull(toKill[0]);

        // Test filter by label.
        validate(h, map -> {
            ClusterNode node = grid(0).cluster().localNode();

            for (Map.Entry<ClusterNode, VisorTxTaskResult> entry : map.entrySet())
                assertEquals(entry.getKey().equals(node) ? 1 : 0, entry.getValue().getInfos().size());
        }, "--tx", "--label", "label1");

        // Test filter by label regex.
        validate(h, map -> {
            ClusterNode node1 = grid(0).cluster().localNode();
            ClusterNode node2 = grid("client").cluster().localNode();

            for (Map.Entry<ClusterNode, VisorTxTaskResult> entry : map.entrySet()) {
                if (entry.getKey().equals(node1)) {
                    assertEquals(1, entry.getValue().getInfos().size());

                    assertEquals("label1", entry.getValue().getInfos().get(0).getLabel());
                }
                else if (entry.getKey().equals(node2)) {
                    assertEquals(1, entry.getValue().getInfos().size());

                    assertEquals("label2", entry.getValue().getInfos().get(0).getLabel());
                }
                else
                    assertTrue(entry.getValue().getInfos().isEmpty());

            }
        }, "--tx", "--label", "^label[0-9]");

        // Test filter by empty label.
        validate(h, map -> {
            VisorTxTaskResult res = map.get(grid(0).localNode());

            for (VisorTxInfo info : res.getInfos())
                assertNull(info.getLabel());

        }, "--tx", "--label", "null");

        // test check minSize
        int minSize = 10;

        validate(h, map -> {
            VisorTxTaskResult res = map.get(grid(0).localNode());

            assertNotNull(res);

            for (VisorTxInfo txInfo : res.getInfos())
                assertTrue(txInfo.getSize() >= minSize);
        }, "--tx", "--min-size", Integer.toString(minSize));

        // test order by size.
        validate(h, map -> {
            VisorTxTaskResult res = map.get(grid(0).localNode());

            assertTrue(res.getInfos().get(0).getSize() >= res.getInfos().get(1).getSize());
        }, "--tx", "--order", "SIZE");

        // test order by duration.
        validate(h, map -> {
            VisorTxTaskResult res = map.get(grid(0).localNode());

            assertTrue(res.getInfos().get(0).getDuration() >= res.getInfos().get(1).getDuration());
        }, "--tx", "--order", "DURATION");

        // test order by start_time.
        validate(h, map -> {
            VisorTxTaskResult res = map.get(grid(0).localNode());

            for (int i = res.getInfos().size() - 1; i > 1; i--)
                assertTrue(res.getInfos().get(i - 1).getStartTime() >= res.getInfos().get(i).getStartTime());
        }, "--tx", "--order", "START_TIME");

        // Trigger topology change and test connection.
        IgniteInternalFuture<?> startFut = multithreadedAsync(() -> {
            try {
                startGrid(2);
            }
            catch (Exception e) {
                fail();
            }
        }, 1, "start-node-thread");

        doSleep(5000); // Give enough time to reach exchange future.

        assertEquals(EXIT_CODE_OK, execute(h, "--tx"));

        // Test kill by xid.
        validate(h, map -> {
                assertEquals(1, map.size());

                Map.Entry<ClusterNode, VisorTxTaskResult> killedEntry = map.entrySet().iterator().next();

                VisorTxInfo info = killedEntry.getValue().getInfos().get(0);

                assertEquals(toKill[0].getXid(), info.getXid());
            }, "--tx", "--kill",
            "--xid", toKill[0].getXid().toString(), // Use saved on first run value.
            "--nodes", grid(0).localNode().consistentId().toString());

        unlockLatch.countDown();

        startFut.get();

        fut.get();

        awaitPartitionMapExchange();

        checkUserFutures();
    }

    /**
     * Simulate uncommitted backup transactions and test rolling back using utility.
     */
    @Test
    public void testKillHangingRemoteTransactions() throws Exception {
        final int cnt = 3;

        startGridsMultiThreaded(cnt);

        Ignite[] clients = new Ignite[] {
            startGrid("client1"),
            startGrid("client2"),
            startGrid("client3"),
            startGrid("client4")
        };

        clients[0].getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME).
            setBackups(2).
            setAtomicityMode(TRANSACTIONAL).
            setWriteSynchronizationMode(FULL_SYNC).
            setAffinity(new RendezvousAffinityFunction(false, 64)));

        awaitPartitionMapExchange();

        for (Ignite client : clients) {
            assertTrue(client.configuration().isClientMode());

            assertNotNull(client.cache(DEFAULT_CACHE_NAME));
        }

        LongAdder progress = new LongAdder();

        AtomicInteger idx = new AtomicInteger();

        int tc = clients.length;

        CountDownLatch lockLatch = new CountDownLatch(1);
        CountDownLatch commitLatch = new CountDownLatch(1);

        Ignite prim = primaryNode(0L, DEFAULT_CACHE_NAME);

        TestRecordingCommunicationSpi primSpi = TestRecordingCommunicationSpi.spi(prim);

        primSpi.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(ClusterNode node, Message message) {
                return message instanceof GridDhtTxFinishRequest;
            }
        });

        Set<IgniteUuid> xidSet = new GridConcurrentHashSet<>();

        IgniteInternalFuture<?> fut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                int id = idx.getAndIncrement();

                Ignite client = clients[id];

                try (Transaction tx = client.transactions().txStart(PESSIMISTIC, READ_COMMITTED, 0, 1)) {
                    xidSet.add(tx.xid());

                    IgniteCache<Long, Long> cache = client.cache(DEFAULT_CACHE_NAME);

                    if (id != 0)
                        U.awaitQuiet(lockLatch);

                    cache.invoke(0L, new IncrementClosure(), null);

                    if (id == 0) {
                        lockLatch.countDown();

                        U.awaitQuiet(commitLatch);

                        doSleep(500); // Wait until candidates will enqueue.
                    }

                    tx.commit();
                }
                catch (Exception e) {
                    assertTrue(X.hasCause(e, TransactionTimeoutException.class));
                }

                progress.increment();

            }
        }, tc, "invoke-thread");

        U.awaitQuiet(lockLatch);

        commitLatch.countDown();

        primSpi.waitForBlocked(clients.length);

        // Unblock only finish messages from clients from 2 to 4.
        primSpi.stopBlock(true, blockedMsg -> {
                GridIoMessage iom = blockedMsg.ioMessage();

                Message m = iom.message();

                if (m instanceof GridDhtTxFinishRequest) {
                    GridDhtTxFinishRequest r = (GridDhtTxFinishRequest)m;

                    return !r.nearNodeId().equals(clients[0].cluster().localNode().id());
                }

                return true;
            }
        );

        // Wait until queue is stable
        for (Ignite ignite : G.allGrids()) {
            if (ignite.configuration().isClientMode())
                continue;

            Collection<IgniteInternalTx> txs = ((IgniteEx)ignite).context().cache().context().tm().activeTransactions();

            waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    for (IgniteInternalTx tx : txs)
                        if (!tx.local()) {
                            IgniteTxEntry entry = tx.writeEntries().iterator().next();

                            GridCacheEntryEx cached = entry.cached();

                            Collection<GridCacheMvccCandidate> candidates = cached.remoteMvccSnapshot();

                            if (candidates.size() != clients.length)
                                return false;
                        }

                    return true;
                }
            }, 10_000);
        }

        CommandHandler h = new CommandHandler();

        // Check listing.
        validate(h, map -> {
            for (int i = 0; i < cnt; i++) {
                IgniteEx grid = grid(i);

                // Skip primary.
                if (grid.localNode().id().equals(prim.cluster().localNode().id()))
                    continue;

                VisorTxTaskResult res = map.get(grid.localNode());

                List<VisorTxInfo> infos = res.getInfos()
                    .stream()
                    .filter(info -> xidSet.contains(info.getNearXid()))
                    .collect(Collectors.toList());

                // Validate queue length on backups.
                assertEquals(clients.length, infos.size());
            }
        }, "--tx");

        // Check kill.
        validate(h, map -> {
            // No-op.
        }, "--tx", "--kill");

        // Wait for all remote txs to finish.
        for (Ignite ignite : G.allGrids()) {
            if (ignite.configuration().isClientMode())
                continue;

            Collection<IgniteInternalTx> txs = ((IgniteEx)ignite).context().cache().context().tm().activeTransactions();

            for (IgniteInternalTx tx : txs)
                if (!tx.local())
                    tx.finishFuture().get();
        }

        // Unblock finish message from client1.
        primSpi.stopBlock(true);

        fut.get();

        Long cur = (Long)clients[0].cache(DEFAULT_CACHE_NAME).get(0L);

        assertEquals(tc - 1, cur.longValue());

        checkUserFutures();
    }

    /**
     * Test baseline add items works via control.sh
     *
     * @throws Exception If failed.
     */
    @Test
    public void testBaselineAddOnNotActiveCluster() throws Exception {
        Ignite ignite = startGrid(1);

        assertFalse(ignite.cluster().active());

        String consistentIDs = getTestIgniteInstanceName(1);

        injectTestSystemOut();

        assertEquals(EXIT_CODE_UNEXPECTED_ERROR, execute("--baseline", "add", consistentIDs));

        assertContains(log, testOut.toString(), "Changing BaselineTopology on inactive cluster is not allowed.");

        consistentIDs =
            getTestIgniteInstanceName(1) + ", " +
                getTestIgniteInstanceName(2) + "," +
                getTestIgniteInstanceName(3);

        assertEquals(EXIT_CODE_INVALID_ARGUMENTS, execute("--baseline", "add", consistentIDs));

        String testOutStr = testOut.toString();

        // Ignite instase 1 can be logged only in arguments list.
        boolean isInstanse1Found = Arrays.stream(testOutStr.split("\n"))
                                        .filter(s -> s.contains("Arguments:"))
                                        .noneMatch(s -> s.contains(getTestIgniteInstanceName() + "1"));

        assertTrue(testOutStr, testOutStr.contains("Node not found for consistent ID:"));
        assertFalse(testOutStr, isInstanse1Found);
    }

    /** */
    @Test
    public void testIdleVerifyCheckCrcFailsOnNotIdleCluster() throws Exception {
        checkpointFreq = 1000L;

        IgniteEx node = startGrids(2);

        node.cluster().active(true);

        IgniteCache<Integer, Integer> cache = node.getOrCreateCache(new CacheConfiguration<Integer, Integer>()
            .setAffinity(new RendezvousAffinityFunction(false, 32))
            .setBackups(1)
            .setName(DEFAULT_CACHE_NAME));

        AtomicBoolean stopFlag = new AtomicBoolean();

        IgniteInternalFuture<?> loadFut = GridTestUtils.runMultiThreadedAsync(() -> {
            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            while (!stopFlag.get() && !Thread.currentThread().isInterrupted())
                cache.put(rnd.nextInt(1000), rnd.nextInt(1000));
        }, 5, "load-thread-");

        try {
            doSleep(checkpointFreq);

            injectTestSystemOut();

            assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify", "--check-crc"));
        }
        finally {
            doSleep(checkpointFreq);

            stopFlag.set(true);

            loadFut.get();
        }

        String out = testOut.toString();

        assertContains(log, out, "The check procedure failed");
        assertContains(log, out, "See log for additional information.");

        String logFileName = (out.split("See log for additional information. ")[1]).split(".txt")[0];

        String logFile = new String(Files.readAllBytes(new File(logFileName + ".txt").toPath()));

        assertContains(log, logFile, GRID_NOT_IDLE_MSG);
    }

    /**
     * Tests that idle verify print partitions info when node failing.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCacheIdleVerifyDumpWhenNodeFailing() throws Exception {
        Ignite ignite = startGrids(3);

        Ignite unstable = startGrid("unstable");

        ignite.cluster().active(true);

        createCacheAndPreload(ignite, 100);

        for (int i = 0; i < 3; i++) {
            TestRecordingCommunicationSpi.spi(unstable).blockMessages(GridJobExecuteResponse.class,
                getTestIgniteInstanceName(i));
        }

        injectTestSystemOut();

        IgniteInternalFuture fut = runAsync(() ->
            assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify", "--dump")));

        TestRecordingCommunicationSpi.spi(unstable).waitForBlocked();

        UUID unstableNodeId = unstable.cluster().localNode().id();

        unstable.close();

        fut.get();

        checkExceptionMessageOnReport(unstableNodeId);
    }

    /**
     * Tests that idle verify print partitions info when several nodes failing at same time.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCacheIdleVerifyDumpWhenSeveralNodesFailing() throws Exception {
        int nodes = 6;

        Ignite ignite = startGrids(nodes);

        List<Ignite> unstableNodes = new ArrayList<>(nodes / 2);

        for (int i = 0; i < nodes; i++) {
            if (i % 2 == 1)
                unstableNodes.add(ignite(i));
        }

        ignite.cluster().active(true);

        createCacheAndPreload(ignite, 100);

        for (Ignite unstable : unstableNodes) {
            for (int i = 0; i < nodes; i++) {
                TestRecordingCommunicationSpi.spi(unstable).blockMessages(GridJobExecuteResponse.class,
                    getTestIgniteInstanceName(i));
            }
        }

        injectTestSystemOut();

        IgniteInternalFuture fut = runAsync(
            () -> assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify", "--dump"))
        );

        List<UUID> unstableNodeIds = new ArrayList<>(nodes / 2);

        for (Ignite unstable : unstableNodes) {
            TestRecordingCommunicationSpi.spi(unstable).waitForBlocked();

            unstableNodeIds.add(unstable.cluster().localNode().id());

            unstable.close();
        }

        fut.get();

        for (UUID unstableId : unstableNodeIds)
            checkExceptionMessageOnReport(unstableId);
    }

    /** */
    @Test
    public void testCacheIdleVerifyCrcWithCorruptedPartition() throws Exception {
        testCacheIdleVerifyWithCorruptedPartition("--cache", "idle_verify", "--check-crc");

        String out = testOut.toString();

        assertContains(log, out, "The check procedure failed on 1 node.");
        assertContains(log, out, "See log for additional information.");
    }

    /** */
    @Test
    public void testCacheIdleVerifyDumpCrcWithCorruptedPartition() throws Exception {
        testCacheIdleVerifyWithCorruptedPartition("--cache", "idle_verify", "--dump", "--check-crc");

        String parts[] = testOut.toString().split("VisorIdleVerifyDumpTask successfully written output to '");

        assertEquals(2, parts.length);

        String dumpFile = parts[1].split("\\.")[0] + ".txt";

        for (String line : Files.readAllLines(new File(dumpFile).toPath()))
            testOut.write(line.getBytes());

        String outputStr = testOut.toString();

        assertContains(log, outputStr, "The check procedure failed on 1 node.");
        assertContains(log, outputStr, "The check procedure has finished, no conflicts have been found.");
    }

    /** */
    private void corruptPartition(File partitionsDir) throws IOException {
        ThreadLocalRandom rand = ThreadLocalRandom.current();

        for (File partFile : partitionsDir.listFiles((d, n) -> n.startsWith("part"))) {
            try (RandomAccessFile raf = new RandomAccessFile(partFile, "rw")) {
                byte[] buf = new byte[1024];

                rand.nextBytes(buf);

                raf.seek(4096 * 2 + 1);

                raf.write(buf);
            }
        }
    }

    /** */
    private void testCacheIdleVerifyWithCorruptedPartition(String... args) throws Exception {
        Ignite ignite = startGrids(2);

        ignite.cluster().active(true);

        createCacheAndPreload(ignite, 1000);

        Serializable consistId = ignite.configuration().getConsistentId();

        File partitionsDir = U.resolveWorkDirectory(
            ignite.configuration().getWorkDirectory(),
            "db/" + consistId + "/cache-" + DEFAULT_CACHE_NAME,
            false
        );

        stopGrid(0);

        corruptPartition(partitionsDir);

        startGrid(0);

        awaitPartitionMapExchange();

        forceCheckpoint();

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute(args));
    }

    /**
     * Try to finds node failed exception message on output report.
     *
     * @param unstableNodeId Unstable node id.
     */
    private void checkExceptionMessageOnReport(UUID unstableNodeId) throws IOException {
        Matcher fileNameMatcher = dumpFileNameMatcher();

        if (fileNameMatcher.find()) {
            String dumpWithConflicts = new String(Files.readAllBytes(Paths.get(fileNameMatcher.group(1))));

            assertContains(log, dumpWithConflicts, "The check procedure failed on nodes:");

            assertContains(log, dumpWithConflicts, "Node ID: " + unstableNodeId);
        }
        else
            fail("Should be found dump with conflicts");
    }

    /**
     * Tests that idle verify print partitions info over none-persistence client caches.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCacheIdleVerifyDumpForCorruptedDataOnNonePersistenceClientCache() throws Exception {
        int parts = 32;

        dataRegionConfiguration = new DataRegionConfiguration()
            .setName("none-persistence-region");

        IgniteEx ignite = startGrids(3);

        ignite.cluster().active(true);

        IgniteCache<Object, Object> cache = ignite.createCache(new CacheConfiguration<>()
            .setAffinity(new RendezvousAffinityFunction(false, parts))
            .setBackups(2)
            .setName(DEFAULT_CACHE_NAME)
            .setDataRegionName("none-persistence-region"));

        // Adding some assignments without deployments.
        for (int i = 0; i < 100; i++)
            cache.put(i, i);

        injectTestSystemOut();

        GridCacheContext<Object, Object> cacheCtx = ignite.cachex(DEFAULT_CACHE_NAME).context();

        corruptDataEntry(cacheCtx, 0, true, false);

        corruptDataEntry(cacheCtx, parts / 2, false, true);

        assertEquals(
            EXIT_CODE_OK,
            execute("--cache", "idle_verify", "--dump", "--cache-filter", "NOT_PERSISTENT")
        );

        Matcher fileNameMatcher = dumpFileNameMatcher();

        if (fileNameMatcher.find()) {
            String dumpWithConflicts = new String(Files.readAllBytes(Paths.get(fileNameMatcher.group(1))));

            assertContains(log, dumpWithConflicts, "found 1 conflict partitions: [counterConflicts=0, " +
                "hashConflicts=1]");
        }
        else
            fail("Should be found dump with conflicts");
    }

    /**
     * @return Build matcher for dump file name.
     */
    @NotNull private Matcher dumpFileNameMatcher() {
        Pattern fileNamePattern = Pattern.compile(".*VisorIdleVerifyDumpTask successfully written output to '(.*)'");

        return fileNamePattern.matcher(testOut.toString());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCacheIdleVerifyMovingParts() throws Exception {
        IgniteEx ignite = startGrids(2);

        ignite.cluster().baselineAutoAdjustEnabled(false);

        ignite.cluster().active(true);

        int parts = 32;

        IgniteCache<Object, Object> cache = ignite.createCache(new CacheConfiguration<>()
            .setAffinity(new RendezvousAffinityFunction(false, parts))
            .setBackups(1)
            .setName(DEFAULT_CACHE_NAME)
            .setRebalanceDelay(10_000));

        for (int i = 0; i < 100; i++)
            cache.put(i, i);

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify"));

        assertContains(log, testOut.toString(), "no conflicts have been found");

        startGrid(2);

        resetBaselineTopology();

        assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify"));

        assertContains(log, testOut.toString(), "MOVING partitions");
    }

    /** */
    @Test
    public void testCacheSequence() throws Exception {
        Ignite ignite = startGrid();

        ignite.cluster().active(true);

        Ignite client = startGrid("client");

        final IgniteAtomicSequence seq1 = client.atomicSequence("testSeq", 1, true);
        seq1.get();

        final IgniteAtomicSequence seq2 = client.atomicSequence("testSeq2", 10, true);
        seq2.get();

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "list", "testSeq.*", "--seq"));

        String out = testOut.toString();

        assertContains(log, out, "testSeq");
        assertContains(log, out, "testSeq2");
    }

    /**
     * @param h Handler.
     * @param validateClo Validate clo.
     * @param args Args.
     */
    private void validate(CommandHandler h, IgniteInClosure<Map<ClusterNode, VisorTxTaskResult>> validateClo,
        String... args) {
        assertEquals(EXIT_CODE_OK, execute(h, args));

        validateClo.apply(h.getLastOperationResult());
    }

    /**
     * @param from From.
     * @param cnt Count.
     */
    private Map<Object, Object> generate(int from, int cnt) {
        Map<Object, Object> map = new TreeMap<>();

        for (int i = 0; i < cnt; i++)
            map.put(i + from, i + from);

        return map;
    }

    /**
     * Test execution of --diagnostic command.
     *
     * @throws Exception if failed.
     */
    @Test
    public void testDiagnosticPageLocksTracker() throws Exception {
        Ignite ignite = startGrid(0, (UnaryOperator<IgniteConfiguration>)cfg -> cfg.setConsistentId("node0/dump"));
        startGrid(1, (UnaryOperator<IgniteConfiguration>)cfg -> cfg.setConsistentId("node1/dump"));
        startGrid(2, (UnaryOperator<IgniteConfiguration>)cfg -> cfg.setConsistentId("node2/dump"));
        startGrid(3, (UnaryOperator<IgniteConfiguration>)cfg -> cfg.setConsistentId("node3/dump"));

        Collection<ClusterNode> nodes = ignite.cluster().nodes();

        List<ClusterNode> nodes0 = new ArrayList<>(nodes);

        ClusterNode node0 = nodes0.get(0);
        ClusterNode node1 = nodes0.get(1);
        ClusterNode node2 = nodes0.get(2);
        ClusterNode node3 = nodes0.get(3);

        ignite.cluster().active(true);

        assertEquals(
            EXIT_CODE_OK,
            execute("--diagnostic")
        );

        assertEquals(
            EXIT_CODE_OK,
            execute("--diagnostic", "help")
        );

        // Dump locks only on connected node to default path.
        assertEquals(
            EXIT_CODE_OK,
            execute("--diagnostic", "pageLocks", "dump")
        );

        // Check file dump in default path.
        checkNumberFiles(defaultDiagnosticDir, 1);

        assertEquals(
            EXIT_CODE_OK,
            execute("--diagnostic", "pageLocks", "dump_log")
        );

        // Dump locks only on connected node to specific path.
        assertEquals(
            EXIT_CODE_OK,
            execute("--diagnostic", "pageLocks", "dump", "--path", customDiagnosticDir.getAbsolutePath())
        );

        // Check file dump in specific path.
        checkNumberFiles(customDiagnosticDir, 1);

        // Dump locks only all nodes.
        assertEquals(
            EXIT_CODE_OK,
            execute("--diagnostic", "pageLocks", "dump", "--all")
        );

        // Current cluster 4 nodes -> 4 files + 1 from previous operation.
        checkNumberFiles(defaultDiagnosticDir, 5);

        assertEquals(
            EXIT_CODE_OK,
            execute("--diagnostic", "pageLocks", "dump_log", "--all")
        );

        assertEquals(
            EXIT_CODE_OK,
            execute("--diagnostic", "pageLocks", "dump",
                "--path", customDiagnosticDir.getAbsolutePath(), "--all")
        );

        // Current cluster 4 nodes -> 4 files + 1 from previous operation.
        checkNumberFiles(customDiagnosticDir, 5);

        // Dump locks only 2 nodes use nodeIds as arg.
        assertEquals(
            EXIT_CODE_OK,
            execute("--diagnostic", "pageLocks", "dump",
                "--nodes", node0.id().toString() + "," + node2.id().toString())
        );

        // Dump locks only for 2 nodes -> 2 files + 5 from previous operation.
        checkNumberFiles(defaultDiagnosticDir, 7);

        // Dump locks only for 2 nodes use constIds as arg.
        assertEquals(
            EXIT_CODE_OK,
            execute("--diagnostic", "pageLocks", "dump",
                "--nodes", node0.consistentId().toString() + "," + node2.consistentId().toString())
        );

        assertEquals(
            EXIT_CODE_OK,
            execute("--diagnostic", "pageLocks", "dump_log",
                "--nodes", node1.id().toString() + "," + node3.id().toString())
        );

        assertEquals(
            EXIT_CODE_OK,
            execute(
                "--diagnostic", "pageLocks", "dump",
                "--path", customDiagnosticDir.getAbsolutePath(),
                "--nodes", node1.consistentId().toString() + "," + node3.consistentId().toString())
        );

        // Dump locks only for 2 nodes -> 2 files + 5 from previous operation.
        checkNumberFiles(customDiagnosticDir, 7);
    }

    /**
     * @param dir Directory.
     * @param numberFiles Number of files.
     */
    private void checkNumberFiles(File dir, int numberFiles) {
        File[] files = dir.listFiles((d, name) -> name.startsWith(ToFileDumpProcessor.PREFIX_NAME));

        assertEquals(numberFiles, files.length);

        for (int i = 0; i < files.length; i++)
            assertTrue(files[i].length() > 0);
    }

    /**
     * Starts several long transactions in order to test --tx command. Transactions will last until unlock latch is
     * released: first transaction will wait for unlock latch directly, some others will wait for key lock acquisition.
     *
     * @param lockLatch Lock latch. Will be released inside body of the first transaction.
     * @param unlockLatch Unlock latch. Should be released externally. First transaction won't be finished until unlock
     * latch is released.
     * @param topChangeBeforeUnlock <code>true</code> should be passed if cluster topology is expected to change between
     * method call and unlock latch release. Commit of the first transaction will be asserted to fail in such case.
     * @return Future to be completed after finish of all started transactions.
     */
    private IgniteInternalFuture<?> startTransactions(
        String testName,
        CountDownLatch lockLatch,
        CountDownLatch unlockLatch,
        boolean topChangeBeforeUnlock
    ) throws Exception {
        IgniteEx client = grid("client");

        AtomicInteger idx = new AtomicInteger();

        return multithreadedAsync(new Runnable() {
            @Override public void run() {
                int id = idx.getAndIncrement();

                switch (id) {
                    case 0:
                        try (Transaction tx = grid(0).transactions().txStart()) {
                            grid(0).cache(DEFAULT_CACHE_NAME).putAll(generate(0, 100));

                            lockLatch.countDown();

                            U.awaitQuiet(unlockLatch);

                            tx.commit();

                            if (topChangeBeforeUnlock)
                                fail("Commit must fail");
                        }
                        catch (Exception e) {
                            if (topChangeBeforeUnlock)
                                assertTrue(X.hasCause(e, TransactionRollbackException.class));
                            else
                                throw e;
                        }

                        break;
                    case 1:
                        U.awaitQuiet(lockLatch);

                        doSleep(3000);

                        try (Transaction tx =
                                 grid(0).transactions().withLabel("label1").txStart(PESSIMISTIC, READ_COMMITTED, Integer.MAX_VALUE, 0)) {
                            grid(0).cache(DEFAULT_CACHE_NAME).putAll(generate(200, 110));

                            grid(0).cache(DEFAULT_CACHE_NAME).put(0, 0);
                        }

                        break;
                    case 2:
                        try (Transaction tx = grid(1).transactions().txStart()) {
                            U.awaitQuiet(lockLatch);

                            grid(1).cache(DEFAULT_CACHE_NAME).put(0, 0);
                        }

                        break;
                    case 3:
                        try (Transaction tx = client.transactions().withLabel("label2").txStart(OPTIMISTIC, READ_COMMITTED, 0, 0)) {
                            U.awaitQuiet(lockLatch);

                            client.cache(DEFAULT_CACHE_NAME).putAll(generate(100, 10));

                            client.cache(DEFAULT_CACHE_NAME).put(0, 0);

                            tx.commit();
                        }

                        break;
                }
            }
        }, 4, "tx-thread-" + testName);
    }

    /** */
    private static class IncrementClosure implements EntryProcessor<Long, Long, Void> {
        /** {@inheritDoc} */
        @Override public Void process(
            MutableEntry<Long, Long> entry,
            Object... arguments
        ) throws EntryProcessorException {
            entry.setValue(entry.exists() ? entry.getValue() + 1 : 0);

            return null;
        }
    }

    /**
     * Corrupts data entry.
     *
     * @param ctx Context.
     * @param key Key.
     * @param breakCntr Break counter.
     * @param breakData Break data.
     */
    private void corruptDataEntry(
        GridCacheContext<Object, Object> ctx,
        Object key,
        boolean breakCntr,
        boolean breakData
    ) {
        int partId = ctx.affinity().partition(key);

        try {
            long updateCntr = ctx.topology().localPartition(partId).updateCounter();

            Object valToPut = ctx.cache().keepBinary().get(key);

            if (breakCntr)
                updateCntr++;

            if (breakData)
                valToPut = valToPut.toString() + " broken";

            // Create data entry
            DataEntry dataEntry = new DataEntry(
                ctx.cacheId(),
                new KeyCacheObjectImpl(key, null, partId),
                new CacheObjectImpl(valToPut, null),
                GridCacheOperation.UPDATE,
                new GridCacheVersion(),
                new GridCacheVersion(),
                0L,
                partId,
                updateCntr
            );

            GridCacheDatabaseSharedManager db = (GridCacheDatabaseSharedManager)ctx.shared().database();

            db.checkpointReadLock();

            try {
                U.invoke(GridCacheDatabaseSharedManager.class, db, "applyUpdate", ctx, dataEntry);
            }
            finally {
                db.checkpointReadUnlock();
            }
        }
        catch (IgniteCheckedException e) {
            e.printStackTrace();
        }
    }

    /** */
    @Test
    public void testKillHangingLocalTransactions() throws Exception {
        Ignite ignite = startGridsMultiThreaded(2);

        ignite.cluster().active(true);

        Ignite client = startGrid("client");

        client.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME).
            setAtomicityMode(TRANSACTIONAL).
            setWriteSynchronizationMode(FULL_SYNC).
            setAffinity(new RendezvousAffinityFunction(false, 64)));

        Ignite prim = primaryNode(0L, DEFAULT_CACHE_NAME);

        // Blocks lock response to near node.
        TestRecordingCommunicationSpi.spi(prim).blockMessages(GridNearLockResponse.class, client.name());

        TestRecordingCommunicationSpi.spi(client).blockMessages(GridNearTxFinishRequest.class, prim.name());

        GridNearTxLocal clientTx = null;

        try (Transaction tx = client.transactions().txStart(PESSIMISTIC, READ_COMMITTED, 2000, 1)) {
            clientTx = ((TransactionProxyImpl)tx).tx();

            client.cache(DEFAULT_CACHE_NAME).put(0L, 0L);

            fail();
        }
        catch (Exception e) {
            assertTrue(X.hasCause(e, TransactionTimeoutException.class));
        }

        assertNotNull(clientTx);

        IgniteEx primEx = (IgniteEx)prim;

        IgniteInternalTx tx0 = primEx.context().cache().context().tm().activeTransactions().iterator().next();

        assertNotNull(tx0);

        CommandHandler h = new CommandHandler();

        validate(h, map -> {
            ClusterNode node = grid(0).cluster().localNode();

            VisorTxTaskResult res = map.get(node);

            for (VisorTxInfo info : res.getInfos())
                assertEquals(tx0.xid(), info.getXid());

            assertEquals(1, map.size());
        }, "--tx", "--xid", tx0.xid().toString(), "--kill");

        tx0.finishFuture().get();

        TestRecordingCommunicationSpi.spi(prim).stopBlock();

        TestRecordingCommunicationSpi.spi(client).stopBlock();

        IgniteInternalFuture<?> nearFinFut = U.field(clientTx, "finishFut");

        nearFinFut.get();

        checkUserFutures();
    }

    /**
     * Verify that in case of setting baseline topology with offline node among others
     * {@link IgniteException} is thrown.
     *
     * @throws Exception If failed.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void setConsistenceIdsWithOfflineBaselineNode() throws Exception {
        Ignite ignite = startGrids(2);

        ignite.cluster().active(true);

        ignite(0).createCache(defaultCacheConfiguration().setNodeFilter(
            (IgnitePredicate<ClusterNode>)node -> node.attribute("some-attr") != null));

        assertEquals(EXIT_CODE_INVALID_ARGUMENTS,
            execute("--baseline", "set", "non-existing-node-id ," + consistentIds(ignite)));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCacheIdleVerifyPrintLostPartitions() throws Exception {
        IgniteEx ignite = startGrids(3);

        ignite.cluster().active(true);

        ignite.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction(false, 16))
            .setCacheMode(PARTITIONED)
            .setPartitionLossPolicy(READ_ONLY_SAFE)
            .setBackups(1));

        try (IgniteDataStreamer streamer = ignite.dataStreamer(DEFAULT_CACHE_NAME)) {
            for (int i = 0; i < 10000; i++)
                streamer.addData(i, new byte[i]);
        }

        String g1Name = grid(1).name();

        stopGrid(1);

        cleanPersistenceDir(g1Name);

        //Start node 2 with empty PDS. Rebalance will be started.
        startGrid(1);

        //During rebalance stop node 3. Rebalance will be stopped which lead to lost partitions.
        stopGrid(2);

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify", "--yes"));

        assertContains(log, testOut.toString(), "LOST partitions:");
    }

    /** @throws Exception If failed. */
    @Test
    public void testMasterKeyChange() throws Exception {
        encryptionEnabled = true;

        injectTestSystemOut();

        Ignite ignite = startGrids(1);

        ignite.cluster().state(ACTIVE);

        createCacheAndPreload(ignite, 10);

        CommandHandler h = new CommandHandler();

        assertEquals(EXIT_CODE_OK, execute(h, "--encryption", "get_master_key_name"));

        assertContains(log, testOut.toString(), ignite.encryption().getMasterKeyName());

        assertEquals(EXIT_CODE_OK, execute(h, "--encryption", "change_master_key", MASTER_KEY_NAME_2));

        assertContains(log, testOut.toString(), "The master key changed.");

        assertEquals(MASTER_KEY_NAME_2, ignite.encryption().getMasterKeyName());

        assertEquals(EXIT_CODE_OK, execute(h, "--encryption", "get_master_key_name"));

        assertContains(log, testOut.toString(), ignite.encryption().getMasterKeyName());

        testOut.reset();

        assertEquals(EXIT_CODE_UNEXPECTED_ERROR,
            execute("--encryption", "change_master_key", "non-existing-master-key-name"));

        assertContains(log, testOut.toString(),
            "Master key change was rejected. Unable to get the master key digest.");
    }

    /** @throws Exception If failed. */
    @Test
    public void testCacheGroupKeyChange() throws Exception {
        encryptionEnabled = true;

        injectTestSystemOut();

        int srvNodes = 2;

        IgniteEx ignite = startGrids(srvNodes);

        startGrid(CLIENT_NODE_NAME_PREFIX);
        startGrid(DAEMON_NODE_NAME_PREFIX);

        ignite.cluster().state(ACTIVE);

        List<Ignite> srvGrids = GridFunc.asList(grid(0), grid(1));

        enableCheckpoints(srvGrids, false);

        createCacheAndPreload(ignite, 1000);

        int ret = execute("--encryption", CACHE_GROUP_KEY_IDS.toString(), DEFAULT_CACHE_NAME);

        assertEquals(EXIT_CODE_OK, ret);
        assertContains(log, testOut.toString(), "Encryption key identifiers for cache: " + DEFAULT_CACHE_NAME);
        assertEquals(srvNodes, countSubstrs(testOut.toString(), "0 (active)"));

        ret = execute("--encryption", CHANGE_CACHE_GROUP_KEY.toString(), DEFAULT_CACHE_NAME);

        assertEquals(EXIT_CODE_OK, ret);
        assertContains(log, testOut.toString(),
            "The encryption key has been changed for the cache group \"" + DEFAULT_CACHE_NAME + '"');

        ret = execute("--encryption", CACHE_GROUP_KEY_IDS.toString(), DEFAULT_CACHE_NAME);

        assertEquals(testOut.toString(), EXIT_CODE_OK, ret);
        assertContains(log, testOut.toString(), "Encryption key identifiers for cache: " + DEFAULT_CACHE_NAME);
        assertEquals(srvNodes, countSubstrs(testOut.toString(), "1 (active)"));

        GridTestUtils.waitForCondition(() -> {
            execute("--encryption", REENCRYPTION_STATUS.toString(), DEFAULT_CACHE_NAME);

            return srvNodes == countSubstrs(testOut.toString(),
                "re-encryption will be completed after the next checkpoint");
        }, getTestTimeout());

        enableCheckpoints(srvGrids, true);
        forceCheckpoint(srvGrids);

        GridTestUtils.waitForCondition(() -> {
            execute("--encryption", REENCRYPTION_STATUS.toString(), DEFAULT_CACHE_NAME);

            return srvNodes == countSubstrs(testOut.toString(), "re-encryption completed or not required");
        }, getTestTimeout());
    }

    /** @throws Exception If failed. */
    @Test
    public void testChangeReencryptionRate() throws Exception {
        int srvNodes = 2;

        IgniteEx ignite = startGrids(srvNodes);

        ignite.cluster().state(ACTIVE);

        injectTestSystemOut();

        int ret = execute("--encryption", REENCRYPTION_RATE.toString());

        assertEquals(EXIT_CODE_OK, ret);
        assertEquals(srvNodes, countSubstrs(testOut.toString(), "re-encryption rate is not limited."));

        double newRate = 0.01;

        ret = execute("--encryption", REENCRYPTION_RATE.toString(), Double.toString(newRate));

        assertEquals(EXIT_CODE_OK, ret);
        assertEquals(srvNodes, countSubstrs(testOut.toString(),
            String.format("re-encryption rate has been limited to %.2f MB/s.", newRate)));

        ret = execute("--encryption", REENCRYPTION_RATE.toString());

        assertEquals(EXIT_CODE_OK, ret);
        assertEquals(srvNodes, countSubstrs(testOut.toString(),
            String.format("re-encryption rate is limited to %.2f MB/s.", newRate)));

        ret = execute("--encryption", REENCRYPTION_RATE.toString(), "0");

        assertEquals(EXIT_CODE_OK, ret);
        assertEquals(srvNodes, countSubstrs(testOut.toString(), "re-encryption rate is not limited."));
    }

    /** @throws Exception If failed. */
    @Test
    public void testReencryptionSuspendAndResume() throws Exception {
        encryptionEnabled = true;
        reencryptSpeed = 0.01;
        reencryptBatchSize = 1;

        int srvNodes = 2;

        IgniteEx ignite = startGrids(srvNodes);

        ignite.cluster().state(ACTIVE);

        injectTestSystemOut();

        createCacheAndPreload(ignite, 10_000);

        ignite.encryption().changeCacheGroupKey(Collections.singleton(DEFAULT_CACHE_NAME)).get();

        assertTrue(isReencryptionStarted(DEFAULT_CACHE_NAME));

        int ret = execute("--encryption", REENCRYPTION_STATUS.toString(), DEFAULT_CACHE_NAME);

        assertEquals(EXIT_CODE_OK, ret);

        Pattern ptrn = Pattern.compile("(?m)Node [-0-9a-f]{36}:\n\\s+(?<left>\\d+) KB of data.+");
        Matcher matcher = ptrn.matcher(testOut.toString());
        int matchesCnt = 0;

        while (matcher.find()) {
            assertEquals(1, matcher.groupCount());

            int pagesLeft = Integer.parseInt(matcher.group("left"));

            assertTrue(pagesLeft > 0);

            matchesCnt++;
        }

        assertEquals(srvNodes, matchesCnt);

        ret = execute("--encryption", REENCRYPTION_SUSPEND.toString(), DEFAULT_CACHE_NAME);

        assertEquals(EXIT_CODE_OK, ret);
        assertEquals(srvNodes, countSubstrs(testOut.toString(),
            "re-encryption of the cache group \"" + DEFAULT_CACHE_NAME + "\" has been suspended."));
        assertFalse(isReencryptionStarted(DEFAULT_CACHE_NAME));

        ret = execute("--encryption", REENCRYPTION_SUSPEND.toString(), DEFAULT_CACHE_NAME);

        assertEquals(EXIT_CODE_OK, ret);
        assertEquals(srvNodes, countSubstrs(testOut.toString(),
            "re-encryption of the cache group \"" + DEFAULT_CACHE_NAME + "\" has already been suspended."));

        ret = execute("--encryption", REENCRYPTION_RESUME.toString(), DEFAULT_CACHE_NAME);

        assertEquals(EXIT_CODE_OK, ret);
        assertEquals(srvNodes, countSubstrs(testOut.toString(),
            "re-encryption of the cache group \"" + DEFAULT_CACHE_NAME + "\" has been resumed."));
        assertTrue(isReencryptionStarted(DEFAULT_CACHE_NAME));

        ret = execute("--encryption", REENCRYPTION_RESUME.toString(), DEFAULT_CACHE_NAME);

        assertEquals(EXIT_CODE_OK, ret);
        assertEquals(srvNodes, countSubstrs(testOut.toString(),
            "re-encryption of the cache group \"" + DEFAULT_CACHE_NAME + "\" has already been resumed."));
    }

    /**
     * @param cacheName Cache name.
     * @return {@code True} if re-encryption of the specified cache is started on all server nodes.
     */
    private boolean isReencryptionStarted(String cacheName) {
        for (Ignite grid : G.allGrids()) {
            ClusterNode locNode = grid.cluster().localNode();

            if (locNode.isClient() || locNode.isDaemon())
                continue;

            if (((IgniteEx)grid).context().encryption().reencryptionFuture(CU.cacheId(cacheName)).isDone())
                return false;
        }

        return true;
    }

    /** @throws Exception If failed. */
    @Test
    public void testMasterKeyChangeOnInactiveCluster() throws Exception {
        encryptionEnabled = true;

        injectTestSystemOut();

        Ignite ignite = startGrids(1);

        CommandHandler h = new CommandHandler();

        assertEquals(EXIT_CODE_OK, execute(h, "--encryption", "get_master_key_name"));

        Object res = h.getLastOperationResult();

        assertEquals(ignite.encryption().getMasterKeyName(), res);

        assertEquals(EXIT_CODE_UNEXPECTED_ERROR, execute(h, "--encryption", "change_master_key", MASTER_KEY_NAME_2));

        assertContains(log, testOut.toString(), "Master key change was rejected. The cluster is inactive.");
    }

    /** @throws Exception If failed. */
    @Test
    public void testClusterSnapshotCreate() throws Exception {
        int keysCnt = 100;
        String snpName = "snapshot_02052020";

        IgniteEx ig = startGrid(0);
        ig.cluster().state(ACTIVE);

        createCacheAndPreload(ig, keysCnt);

        CommandHandler h = new CommandHandler();

        assertEquals(EXIT_CODE_OK, execute(h, "--snapshot", "create", snpName));

        assertTrue("Waiting for snapshot operation end failed.",
            waitForCondition(() ->
                    ig.context().metric().registry(SNAPSHOT_METRICS)
                        .<LongMetric>findMetric("LastSnapshotEndTime").value() > 0,
                getTestTimeout()));

        assertContains(log, (String)h.getLastOperationResult(), snpName);

        stopAllGrids();

        IgniteConfiguration cfg = optimize(getConfiguration(getTestIgniteInstanceName(0)));
        cfg.setWorkDirectory(Paths.get(resolveSnapshotWorkDirectory(cfg).getAbsolutePath(), snpName).toString());

        Ignite snpIg = startGrid(cfg);
        snpIg.cluster().state(ACTIVE);

        List<Integer> range = IntStream.range(0, keysCnt).boxed().collect(Collectors.toList());

        snpIg.cache(DEFAULT_CACHE_NAME).forEach(e -> range.remove((Integer)e.getKey()));
        assertTrue("Snapshot must contains cache data [left=" + range + ']', range.isEmpty());
    }

    /** @throws Exception If failed. */
    @Test
    public void testClusterSnapshotOnInactive() throws Exception {
        injectTestSystemOut();

        startGrids(1);

        assertEquals(EXIT_CODE_UNEXPECTED_ERROR, execute(new CommandHandler(), "--snapshot", "create",
            "testSnapshotName"));

        assertContains(log, testOut.toString(), "Snapshot operation has been rejected. The cluster is inactive.");
    }

    /** @throws Exception If fails. */
    @Test
    public void testCancelSnapshot() throws Exception {
        IgniteEx srv = startGrid(0);
        IgniteEx startCli = startClientGrid(CLIENT_NODE_NAME_PREFIX);

        srv.cluster().state(ACTIVE);

        createCacheAndPreload(startCli, 100);

        CommandHandler h = new CommandHandler();

        doSnapshotCancellationTest(startCli, Collections.singletonList(srv), startCli.cache(DEFAULT_CACHE_NAME),
            snpName -> assertEquals(EXIT_CODE_OK, execute(h, "--snapshot", "cancel", snpName)));
    }

    /** @throws Exception If fails. */
    @Test
    public void testCheckSnapshot() throws Exception {
        String snpName = "snapshot_02052020";

        IgniteEx ig = startGrid(0);
        ig.cluster().state(ACTIVE);

        createCacheAndPreload(ig, 1000);

        snp(ig).createSnapshot(snpName)
            .get();

        CommandHandler h = new CommandHandler();

        assertEquals(EXIT_CODE_OK, execute(h, "--snapshot", "check", snpName));

        StringBuilder sb = new StringBuilder();

        ((IdleVerifyResultV2)h.getLastOperationResult()).print(sb::append, true);

        assertContains(log, sb.toString(), "The check procedure has finished, no conflicts have been found");
    }

    /** @throws Exception If fails. */
    @Test
    public void testSnapshotRestore() throws Exception {
        int keysCnt = 100;
        String snpName = "snapshot_02052020";
        String cacheName1 = "cache1";
        String cacheName2 = "cache2";
        String cacheName3 = "cache3";

        IgniteEx ig = startGrids(2);

        ig.cluster().state(ACTIVE);

        injectTestSystemOut();

        createCacheAndPreload(ig, cacheName1, keysCnt, 32, null);
        createCacheAndPreload(ig, cacheName2, keysCnt, 32, null);
        createCacheAndPreload(ig, cacheName3, keysCnt, 32, null);

        ig.snapshot().createSnapshot(snpName).get(getTestTimeout());

        IgniteCache<Integer, Integer> cache1 = ig.cache(cacheName1);
        IgniteCache<Integer, Integer> cache2 = ig.cache(cacheName2);
        IgniteCache<Integer, Integer> cache3 = ig.cache(cacheName3);

        cache1.destroy();
        cache2.destroy();
        cache3.destroy();

        awaitPartitionMapExchange();

        assertNull(ig.cache(cacheName1));
        assertNull(ig.cache(cacheName2));
        assertNull(ig.cache(cacheName3));

        CommandHandler h = new CommandHandler();

        // Restore single cache group.
        assertEquals(EXIT_CODE_OK, execute(h, "--snapshot", "restore", snpName, "--start", cacheName1));
        assertContains(log, testOut.toString(),
            "Snapshot cache group restore operation started [snapshot=" + snpName + ", group(s)=" + cacheName1 + ']');

        waitForCondition(() -> ig.cache(cacheName1) != null, getTestTimeout());

        cache1 = ig.cache(cacheName1);

        assertNotNull(cache1);

        for (int i = 0; i < keysCnt; i++)
            assertEquals(cacheName1, Integer.valueOf(i), cache1.get(i));

        cache1.destroy();

        awaitPartitionMapExchange();

        assertNull(ig.cache(cacheName1));
        assertNull(ig.cache(cacheName2));
        assertNull(ig.cache(cacheName3));

        // Restore two (of three) groups of caches.
        assertEquals(EXIT_CODE_OK, execute(h, "--snapshot", "restore", snpName, "--start", cacheName1 + ',' + cacheName2));
        assertContains(log, testOut.toString(),
            "Snapshot cache group restore operation started [snapshot=" + snpName + ", group(s)=");

        waitForCondition(() -> ig.cache(cacheName1) != null, getTestTimeout());
        waitForCondition(() -> ig.cache(cacheName2) != null, getTestTimeout());

        cache1 = ig.cache(cacheName1);
        cache2 = ig.cache(cacheName2);

        assertNotNull(cache1);
        assertNotNull(cache2);

        for (int i = 0; i < keysCnt; i++) {
            assertEquals(cacheName1, Integer.valueOf(i), cache1.get(i));
            assertEquals(cacheName2, Integer.valueOf(i), cache2.get(i));
        }

        cache1.destroy();
        cache2.destroy();

        awaitPartitionMapExchange();

        assertNull(ig.cache(cacheName1));
        assertNull(ig.cache(cacheName2));
        assertNull(ig.cache(cacheName3));

        // Restore all public cache groups.
        assertEquals(EXIT_CODE_OK, execute(h, "--snapshot", "restore", snpName, "--start"));
        assertContains(log, testOut.toString(),
            "Snapshot cache group restore operation started [snapshot=" + snpName + ']');

        waitForCondition(() -> ig.cache(cacheName1) != null, getTestTimeout());
        waitForCondition(() -> ig.cache(cacheName2) != null, getTestTimeout());
        waitForCondition(() -> ig.cache(cacheName3) != null, getTestTimeout());

        cache1 = ig.cache(cacheName1);
        cache2 = ig.cache(cacheName2);
        cache3 = ig.cache(cacheName3);

        assertNotNull(cache1);
        assertNotNull(cache2);
        assertNotNull(cache3);

        for (int i = 0; i < keysCnt; i++) {
            assertEquals(cacheName1, Integer.valueOf(i), cache1.get(i));
            assertEquals(cacheName2, Integer.valueOf(i), cache2.get(i));
            assertEquals(cacheName3, Integer.valueOf(i), cache2.get(i));
        }
    }

    /** @throws Exception If fails. */
    @Test
    public void testSnapshotRestoreCancelAndStatus() throws Exception {
        int keysCnt = 10_000;
        String snpName = "snapshot_25052021";
        String missingSnpName = "snapshot_MISSING";

        IgniteEx ig = startGrids(2);

        ig.cluster().state(ACTIVE);

        injectTestSystemOut();

        createCacheAndPreload(ig, keysCnt);

        ig.snapshot().createSnapshot(snpName).get(getTestTimeout());

        IgniteCache<Integer, Integer> cache1 = ig.cache(DEFAULT_CACHE_NAME);

        cache1.destroy();

        CommandHandler h = new CommandHandler();

        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(grid(1));

        spi.blockMessages((node, msg) -> msg instanceof SingleNodeMessage &&
            ((SingleNodeMessage<?>)msg).type() == RESTORE_CACHE_GROUP_SNAPSHOT_PREPARE.ordinal());

        // Restore single cache group.
        assertEquals(EXIT_CODE_OK, execute(h, "--snapshot", "restore", snpName, "--start", DEFAULT_CACHE_NAME));
        assertContains(log, testOut.toString(),
            "Snapshot cache group restore operation started [snapshot=" + snpName + ", group(s)=" + DEFAULT_CACHE_NAME + ']');

        assertEquals(EXIT_CODE_OK, execute(h, "--snapshot", "restore", snpName, "--status"));
        assertContains(log, testOut.toString(),
            "Snapshot cache group restore operation is running [snapshot=" + snpName + ']');

        // Check wrong snapshot name.
        assertEquals(EXIT_CODE_OK, execute(h, "--snapshot", "restore", missingSnpName, "--status"));
        assertContains(log, testOut.toString(),
            "Snapshot cache group restore operation is NOT running [snapshot=" + missingSnpName + ']');

        assertEquals(EXIT_CODE_OK, execute(h, "--snapshot", "restore", missingSnpName, "--cancel"));
        assertContains(log, testOut.toString(),
            "Snapshot cache group restore operation is not in progress [snapshot=" + missingSnpName + ']');

        GridTestUtils.runAsync(() -> {
            // Wait for the process to be interrupted.
            AtomicReference<?> errRef = U.field((Object)U.field((Object)U.field(
                grid(0).context().cache().context().snapshotMgr(), "restoreCacheGrpProc"), "opCtx"), "err");

            waitForCondition(() -> errRef.get() != null, getTestTimeout());

            spi.stopBlock();

            return null;
        });

        assertEquals(EXIT_CODE_OK, execute(h, "--snapshot", "restore", snpName, "--cancel"));
        assertContains(log, testOut.toString(),
            "Snapshot cache group restore operation canceled [snapshot=" + snpName + ']');

        assertEquals(EXIT_CODE_OK, execute(h, "--snapshot", "restore", snpName, "--status"));
        assertContains(log, testOut.toString(),
            "Snapshot cache group restore operation is NOT running [snapshot=" + snpName + ']');
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_PDS_SKIP_CHECKPOINT_ON_NODE_STOP, value = "true")
    public void testCleaningGarbageAfterCacheDestroyedAndNodeStop_ControlConsoleUtil() throws Exception {
        new IgniteCacheGroupsWithRestartsTest().testFindAndDeleteGarbage(this::executeTaskViaControlConsoleUtil);
    }

    /**
     * Verification of successful warm-up stop.
     * <p/>
     * Steps:
     * 1)Starting node with warm-up;
     * 2)Stop warm-up;
     * 3)Waiting for a successful stop of warm-up and start of node.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSuccessStopWarmUp() throws Exception {
        WarmUpTestPluginProvider provider = new WarmUpTestPluginProvider();

        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(0)).setPluginProviders(provider);
        cfg.getDataStorageConfiguration().setDefaultWarmUpConfiguration(new BlockedWarmUpConfiguration());

        cfg.getConnectorConfiguration().setHost("localhost");

        IgniteInternalFuture<IgniteEx> fut = runAsync(() -> startGrid(cfg));

        BlockedWarmUpStrategy blockedWarmUpStgy = (BlockedWarmUpStrategy)provider.strats.get(1);

        try {
            U.await(blockedWarmUpStgy.startLatch, 60, TimeUnit.SECONDS);

            // Arguments --user and --password are needed for additional sending of the GridClientAuthenticationRequest.
            assertEquals(EXIT_CODE_OK, execute("--warm-up", "--stop", "--yes", "--user", "user", "--password", "123"));

            assertEquals(0, blockedWarmUpStgy.stopLatch.getCount());
        }
        finally {
            blockedWarmUpStgy.stopLatch.countDown();

            fut.get(60_000);
        }
    }

    /**
     * Check that command will not be executed because node has already started.
     * <p/>
     * Steps:
     * 1)Starting node;
     * 2)Attempt to stop warm-up;
     * 3)Waiting for an error because node has already started.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testFailStopWarmUp() throws Exception {
        startGrid(0);

        assertEquals(EXIT_CODE_UNEXPECTED_ERROR, execute("--warm-up", "--stop", "--yes"));
    }

    /**
     * @param ignite Ignite to execute task on.
     * @param delFoundGarbage If clearing mode should be used.
     * @return Result of task run.
     */
    private VisorFindAndDeleteGarbageInPersistenceTaskResult executeTaskViaControlConsoleUtil(
        IgniteEx ignite,
        boolean delFoundGarbage
    ) {
        CommandHandler hnd = new CommandHandler();

        List<String> args = new ArrayList<>(Arrays.asList("--yes", "--port", "11212", "--cache", "find_garbage",
            ignite.localNode().id().toString()));

        if (delFoundGarbage)
            args.add(FindAndDeleteGarbageArg.DELETE.argName());

        hnd.execute(args);

        return hnd.getLastOperationResult();
    }

    /**
     * @param str String.
     * @param substr Substring to find in the specified string.
     * @return The number of substrings found in the specified string.
     */
    private int countSubstrs(String str, String substr) {
        int cnt = 0;

        for (int off = 0; (off = str.indexOf(substr, off)) != -1; off++)
            ++cnt;

        return cnt;
    }
}
