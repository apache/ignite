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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeOrHaltFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.management.cache.IdleVerifyResult;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.events.EventType.EVTS_CLUSTER_SNAPSHOT;
import static org.apache.ignite.events.EventType.EVT_CLUSTER_SNAPSHOT_RESTORE_FINISHED;
import static org.apache.ignite.events.EventType.EVT_CLUSTER_SNAPSHOT_RESTORE_STARTED;
import static org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree.partId;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/** */
public class IgniteSnapshotRestoreFromRemoteTest extends IgniteClusterSnapshotRestoreBaseTest {
    /** */
    private static final String FIRST_CLUSTER_PREFIX = "one_";

    /** */
    private static final String SECOND_CLUSTER_PREFIX = "two_";

    /** */
    private static final String CACHE_WITH_NODE_FILTER = "cacheWithFilter";

    /** */
    private static final int GRIDS = 6;

    /** Node filter filter test restoring on some nodes only. */
    private static final IgnitePredicate<ClusterNode> ZERO_SUFFIX_NODE_FILTER = new IgnitePredicate<ClusterNode>() {
        @Override public boolean apply(ClusterNode node) {
            return node.consistentId().toString().endsWith("0");
        }
    };

    /** {@code true} if snapshot parts has been initialized on test-class startup. */
    private static boolean inited;

    /** Snapshot parts on dedicated cluster. Each part has its own local directory. */
    private static final Set<Path> snpParts = new HashSet<>();

    /** */
    private static final Function<String, BiFunction<Integer, IgniteConfiguration, String>> CLUSTER_DIR =
        new Function<String, BiFunction<Integer, IgniteConfiguration, String>>() {
            @Override public BiFunction<Integer, IgniteConfiguration, String> apply(String prefix) {
                return (id, cfg) -> Paths.get(defaultWorkDirectory().toString(),
                    prefix + U.maskForFileName(cfg.getIgniteInstanceName())).toString();
            }
        };

    /** Cache value builder. */
    private final Function<Integer, Object> valBuilder = String::valueOf;

    /** */
    private String changedConsistentId;

    /** */
    private boolean usePairedConnections;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (!F.isEmpty(changedConsistentId))
            cfg.setConsistentId(cfg.getConsistentId() + changedConsistentId);

        cfg.setFailureHandler(new StopNodeOrHaltFailureHandler());

        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setUsePairedConnections(usePairedConnections);

        return cfg;
    }

    /** @throws Exception If fails. */
    @Before
    public void prepareDedicatedSnapshot() throws Exception {
        if (!inited) {
            cleanupDedicatedPersistenceDirs(FIRST_CLUSTER_PREFIX);

            CacheConfiguration<Integer, Object> cacheCfg1 =
                txCacheConfig(new CacheConfiguration<Integer, Object>(CACHE1)).setGroupName(SHARED_GRP);

            CacheConfiguration<Integer, Object> cacheCfg2 =
                txCacheConfig(new CacheConfiguration<Integer, Object>(CACHE2)).setGroupName(SHARED_GRP);

            CacheConfiguration<Integer, Object> cacheCfg3 =
                txCacheConfig(new CacheConfiguration<Integer, Object>(CACHE_WITH_NODE_FILTER))
                    .setBackups(1)
                    .setNodeFilter(ZERO_SUFFIX_NODE_FILTER);

            IgniteEx ignite = startDedicatedGridsWithCache(FIRST_CLUSTER_PREFIX, GRIDS, CACHE_KEYS_RANGE, valBuilder,
                dfltCacheCfg.setBackups(0), cacheCfg1, cacheCfg2, cacheCfg3);

            createAndCheckSnapshot(ignite, SNAPSHOT_NAME, null, TIMEOUT);

            awaitPartitionMapExchange();
            stopAllGrids();

            snpParts.addAll(findSnapshotParts(FIRST_CLUSTER_PREFIX, SNAPSHOT_NAME));

            inited = true;
        }

        beforeTestSnapshot();
        cleanupDedicatedPersistenceDirs(SECOND_CLUSTER_PREFIX);
    }

    /** @throws Exception If fails. */
    @After
    public void afterSwitchSnapshot() throws Exception {
        afterTestSnapshot();
        cleanupDedicatedPersistenceDirs(SECOND_CLUSTER_PREFIX);
    }

    /** */
    @AfterClass
    public static void cleanupSnapshot() {
        snpParts.forEach(U::delete);
        cleanupDedicatedPersistenceDirs(FIRST_CLUSTER_PREFIX);
    }

    /** @throws Exception If failed. */
    @Test
    public void testRestoreWithPairedConnections() throws Exception {
        changedConsistentId = "_new";

        usePairedConnections = true;

        IgniteEx scc = startDedicatedGrids(SECOND_CLUSTER_PREFIX, GRIDS);

        scc.cluster().state(ClusterState.ACTIVE);

        copyAndShuffle(snpParts, G.allGrids());

        grid(0).cache(DEFAULT_CACHE_NAME).destroy();

        awaitPartitionMapExchange();

        // Restore all cache groups.
        grid(0).snapshot().restoreSnapshot(SNAPSHOT_NAME, null).get(TIMEOUT);

        assertCacheKeys(scc.cache(DEFAULT_CACHE_NAME), CACHE_KEYS_RANGE);
    }

    /** @throws Exception If failed. */
    @Test
    public void testRestoreAllGroups() throws Exception {
        IgniteEx scc = startDedicatedGrids(SECOND_CLUSTER_PREFIX, 2);
        scc.cluster().state(ClusterState.ACTIVE);

        copyAndShuffle(snpParts, G.allGrids());

        grid(0).cache(DEFAULT_CACHE_NAME).destroy();

        for (Ignite g : G.allGrids())
            TestRecordingCommunicationSpi.spi(g).record(SnapshotFilesRequestMessage.class);

        // Restore all cache groups.
        grid(0).snapshot().restoreSnapshot(SNAPSHOT_NAME, null).get(TIMEOUT);

        awaitPartitionMapExchange(true, true, null, true);

        assertCacheKeys(scc.cache(DEFAULT_CACHE_NAME), CACHE_KEYS_RANGE);
        assertCacheKeys(scc.cache(CACHE1), CACHE_KEYS_RANGE);
        assertCacheKeys(scc.cache(CACHE2), CACHE_KEYS_RANGE);

        waitForEvents(EVT_CLUSTER_SNAPSHOT_RESTORE_STARTED, EVT_CLUSTER_SNAPSHOT_RESTORE_FINISHED);

        List<Object> msgs = new ArrayList<>();

        for (Ignite g : G.allGrids())
            msgs.addAll(TestRecordingCommunicationSpi.spi(g).recordedMessages(true));

        assertPartitionsDuplicates(msgs);
    }

    /** @throws Exception If failed. */
    @Test
    public void testRestoreFromAnEmptyNode() throws Exception {
        startDedicatedGrids(SECOND_CLUSTER_PREFIX, 2);

        copyAndShuffle(snpParts, G.allGrids());

        // Start a new node without snapshot working directory.
        IgniteEx emptyNode = startDedicatedGrid(SECOND_CLUSTER_PREFIX, 2);

        emptyNode.cluster().state(ClusterState.ACTIVE);

        emptyNode.cache(DEFAULT_CACHE_NAME).destroy();
        awaitPartitionMapExchange();

        // Ensure that the snapshot check command succeeds.
        IdleVerifyResult res = emptyNode.context().cache().context().snapshotMgr()
            .checkSnapshot(SNAPSHOT_NAME, null).get(TIMEOUT).idleVerifyResult();

        StringBuilder buf = new StringBuilder();
        res.print(buf::append, true);

        assertTrue(F.isEmpty(res.exceptions()));
        assertPartitionsSame(res);
        assertContains(log, buf.toString(), "The check procedure has finished, no conflicts have been found");

        // Restore all cache groups.
        emptyNode.snapshot().restoreSnapshot(SNAPSHOT_NAME, null).get(TIMEOUT);

        awaitPartitionMapExchange(true, true, null, true);

        for (Ignite grid : G.allGrids()) {
            assertCacheKeys(grid.cache(DEFAULT_CACHE_NAME), CACHE_KEYS_RANGE);
            assertCacheKeys(grid.cache(CACHE1), CACHE_KEYS_RANGE);
            assertCacheKeys(grid.cache(CACHE2), CACHE_KEYS_RANGE);
        }
    }

    /** @throws Exception If failed. */
    @Test
    public void testRestoreNoRebalance() throws Exception {
        IgniteEx scc = startDedicatedGrids(SECOND_CLUSTER_PREFIX, 2);
        scc.cluster().state(ClusterState.ACTIVE);

        copyAndShuffle(snpParts, G.allGrids());

        grid(0).cache(DEFAULT_CACHE_NAME).destroy();

        for (Ignite g : G.allGrids())
            TestRecordingCommunicationSpi.spi(g).record(GridDhtPartitionDemandMessage.class);

        grid(0).snapshot().restoreSnapshot(SNAPSHOT_NAME, Collections.singleton(CACHE_WITH_NODE_FILTER)).get(TIMEOUT);

        awaitPartitionMapExchange(true, true, null, true);

        assertCacheKeys(scc.cache(CACHE_WITH_NODE_FILTER), CACHE_KEYS_RANGE);
        waitForEvents(EVT_CLUSTER_SNAPSHOT_RESTORE_FINISHED);

        for (Ignite g : G.allGrids())
            assertTrue(TestRecordingCommunicationSpi.spi(g).recordedMessages(true).isEmpty());
    }

    /** @throws Exception If failed. */
    @Test
    public void testSnapshotCachesStoppedIfLoadingFailOnRemote() throws Exception {
        IgniteEx scc = startDedicatedGrids(SECOND_CLUSTER_PREFIX, 2);
        scc.cluster().state(ClusterState.ACTIVE);

        copyAndShuffle(snpParts, G.allGrids());

        grid(0).cache(DEFAULT_CACHE_NAME).destroy();

        IgniteSnapshotManager mgr = snp(grid(1));
        mgr.remoteSnapshotSenderFactory(new BiFunction<String, UUID, SnapshotSender>() {
            @Override public SnapshotSender apply(String s, UUID uuid) {
                return new DelegateSnapshotSender(log, mgr.snapshotExecutorService(), mgr.remoteSnapshotSenderFactory(s, uuid)) {
                    @Override public void sendPart0(File part, File to, GroupPartitionId pair, Long length) {
                        if (partId(part) > 0)
                            throw new IgniteException("Test exception. Uploading partition file failed: " + pair);

                        super.sendPart0(part, to, pair, length);
                    }
                };
            }
        });

        IgniteFuture<?> fut = grid(0).snapshot().restoreSnapshot(SNAPSHOT_NAME, null);

        GridTestUtils.assertThrowsAnyCause(log,
            () -> fut.get(TIMEOUT),
            IgniteException.class,
            "Test exception. Uploading partition file failed");
        assertNull(scc.cache(DEFAULT_CACHE_NAME));
        ensureCacheAbsent(dfltCacheCfg);
    }

    /** @throws Exception If failed. */
    @Test
    public void testRestoreConnectionLost() throws Exception {
        IgniteEx coord = startDedicatedGrids(SECOND_CLUSTER_PREFIX, 2);

        copyAndShuffle(snpParts, G.allGrids());

        // Start a new node without snapshot working directory.
        IgniteEx emptyNode = startDedicatedGrid(SECOND_CLUSTER_PREFIX, 2);

        emptyNode.cluster().state(ClusterState.ACTIVE);

        emptyNode.cache(DEFAULT_CACHE_NAME).destroy();

        awaitPartitionMapExchange();

        CountDownLatch restoreStarted = new CountDownLatch(1);
        CountDownLatch nodeStopped = new CountDownLatch(1);

        IgniteSnapshotManager mgr = snp(coord);

        mgr.remoteSnapshotSenderFactory(new BiFunction<String, UUID, SnapshotSender>() {
            @Override public SnapshotSender apply(String s, UUID uuid) {
                return new DelegateSnapshotSender(log, mgr.snapshotExecutorService(), mgr.remoteSnapshotSenderFactory(s, uuid)) {
                    @Override public void sendPart0(File part, File to, GroupPartitionId pair, Long length) {
                        delegate.sendPart0(part, to, pair, length);

                        restoreStarted.countDown();

                        try {
                            nodeStopped.await(TIMEOUT, TimeUnit.MILLISECONDS);
                        }
                        catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                };
            }
        });

        // Restore all cache groups.
        IgniteFuture<Void> fut = emptyNode.snapshot().restoreSnapshot(SNAPSHOT_NAME, null);

        restoreStarted.await(TIMEOUT, TimeUnit.MILLISECONDS);

        coord.close();

        nodeStopped.countDown();

        assertThrowsWithCause(() -> fut.get(TIMEOUT), IgniteException.class);
    }

    /**
     * @param snpParts Snapshot parts.
     * @param toNodes List of toNodes to copy parts to.
     */
    private static void copyAndShuffle(Set<Path> snpParts, List<Ignite> toNodes) {
        AtomicInteger cnt = new AtomicInteger();

        snpParts.forEach(p -> {
            try {
                IgniteEx loc = (IgniteEx)toNodes.get(cnt.getAndIncrement() % toNodes.size());
                String snpName = p.getFileName().toString();

                U.copy(p.toFile(),
                    Paths.get(loc.context().pdsFolderResolver().fileTree().snapshotsRoot().getAbsolutePath(), snpName).toFile(),
                    false);
            }
            catch (IOException e) {
                throw new IgniteException(e);
            }
        });
    }

    /**
     * @param clusterPrefix Array of prefixes to clean up directories.
     */
    private static void cleanupDedicatedPersistenceDirs(String... clusterPrefix) {
        for (String prefix : clusterPrefix) {
            try (DirectoryStream<Path> ds = Files.newDirectoryStream(defaultWorkDirectory(),
                path -> Files.isDirectory(path) && path.getFileName().toString().toLowerCase().startsWith(prefix))
            ) {
                for (Path dir : ds)
                    U.delete(dir);
            }
            catch (IOException e) {
                throw new IgniteException(e);
            }
        }
    }

    /**
     * @return Collection of dedicated snapshot paths located in Ignite working directory.
     */
    private static Set<Path> findSnapshotParts(String prefix, String snpName) {
        Set<Path> snpPaths = new HashSet<>();

        try (DirectoryStream<Path> ds = Files.newDirectoryStream(defaultWorkDirectory(),
            path -> Files.isDirectory(path) && path.getFileName().toString().toLowerCase().startsWith(prefix))
        ) {
            for (Path dir : ds)
                snpPaths.add(searchDirectoryRecursively(dir, snpName)
                    .orElseThrow(() -> new IgniteException("Snapshot not found in the Ignite work directory " +
                        "[dir=" + dir.toString() + ", snpName=" + snpName + ']')));

            return snpPaths;
        }
        catch (IOException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * @param grids Number of ignite instances to start.
     * @param keys Number of keys to create.
     * @param valMapper Factory which produces values.
     * @param <V> Cache value type.
     * @return Ignite coordinator instance.
     * @throws Exception If fails.
     */
    private <V> IgniteEx startDedicatedGridsWithCache(
        String prefix,
        int grids,
        int keys,
        Function<Integer, V> valMapper,
        CacheConfiguration<Integer, V>... ccfgs
    ) throws Exception {
        return startGridsWithCache(grids,
            keys,
            valMapper,
            CLUSTER_DIR.apply(prefix),
            ccfgs);
    }

    /**
     * @param grids Number of ignite instances to start.
     * @return Ignite coordinator instance.
     * @throws Exception If fails.
     */
    private IgniteEx startDedicatedGrids(String prefix, int grids) throws Exception {
        for (int g = 0; g < grids; g++)
            startDedicatedGrid(prefix, g);

        grid(0).events().localListen(e -> locEvts.add(e.type()), EVTS_CLUSTER_SNAPSHOT);

        return grid(0);
    }

    /**
     * @param prefix Grid work directory prefix.
     * @param id Grid index.
     * @return Grid instance.
     * @throws Exception If fails.
     */
    private IgniteEx startDedicatedGrid(String prefix, int id) throws Exception {
        IgniteConfiguration cfg = optimize(getConfiguration(getTestIgniteInstanceName(id)));
        cfg.setWorkDirectory(CLUSTER_DIR.apply(prefix).apply(id, cfg));

        return startGrid(cfg);
    }

    /**
     * @return Default work directory.
     */
    private static Path defaultWorkDirectory() {
        try {
            return Paths.get(U.defaultWorkDirectory());
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** */
    private static void assertPartitionsDuplicates(List<Object> msgs) {
        List<GroupPartitionId> all = new ArrayList<>();

        for (Object o : msgs) {
            SnapshotFilesRequestMessage msg0 = (SnapshotFilesRequestMessage)o;
            Map<Integer, Set<Integer>> parts = msg0.parts();

            for (Map.Entry<Integer, Set<Integer>> e : parts.entrySet()) {
                for (Integer partId : e.getValue())
                    all.add(new GroupPartitionId(e.getKey(), partId));
            }
        }

        assertEquals(all.size(), new HashSet<>(all).size());
    }
}
