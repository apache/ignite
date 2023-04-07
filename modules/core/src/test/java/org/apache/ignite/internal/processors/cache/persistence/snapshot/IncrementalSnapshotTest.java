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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.GridLocalConfigManager;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.util.distributed.DistributedProcess;
import org.apache.ignite.internal.util.distributed.SingleNodeMessage;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.snapshotMetaFileName;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RESTORE_CACHE_GROUP_SNAPSHOT_PRELOAD;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RESTORE_CACHE_GROUP_SNAPSHOT_PREPARE;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.RESTORE_INCREMENTAL_SNAPSHOT_START;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.junit.Assume.assumeFalse;

/**
 * Basic tests for incremental snapshots.
 */
public class IncrementalSnapshotTest extends AbstractSnapshotSelfTest {
    /** */
    public static final int GRID_CNT = 3;

    /** */
    public static final String OTHER_CACHE = "other-cache";

    /** */
    public static final String GROUPED_CACHE = "my-grouped-cache2";

    /** @see DataStorageConfiguration#isWalCompactionEnabled() */
    public boolean walCompactionEnabled = true;

    /** */
    private AtomicInteger cntr = new AtomicInteger();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (cfg.isClientMode())
            return cfg;

        cfg.getDataStorageConfiguration()
            .setWalCompactionEnabled(walCompactionEnabled)
            .setWalSegmentSize((int)U.MB);

        return cfg;
    }

    /** */
    @Test
    public void testCreation() throws Exception {
        assumeFalse("https://issues.apache.org/jira/browse/IGNITE-17819", encryption);

        IgniteEx srv = startGridsWithCache(
            GRID_CNT,
            CACHE_KEYS_RANGE,
            key -> new Account(key, key),
            new CacheConfiguration<>(DEFAULT_CACHE_NAME)
        );

        File snpDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), "ex_snapshots", true);

        assertTrue("Target directory is not empty: " + snpDir, F.isEmpty(snpDir.list()));

        IgniteEx cli = startClientGrid(
            GRID_CNT,
            (UnaryOperator<IgniteConfiguration>)
                cfg -> cfg.setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME))
        );

        File exSnpDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), "ex_snapshots", true);

        for (boolean client : new boolean[] {false, true}) {
            IgniteSnapshotManager snpCreate = snp(client ? cli : srv);

            for (File snpPath : new File[] {null, exSnpDir}) {
                if (client && snpPath != null) // Set snapshot path not supported for snapshot from client nodes.
                    continue;

                String snpName = SNAPSHOT_NAME + "_" + client + "_" + (snpPath == null ? "" : snpPath.getName());

                snpCreate
                    .createSnapshot(snpName, snpPath == null ? null : snpPath.getAbsolutePath(), false, onlyPrimary)
                    .get(TIMEOUT);

                checkSnapshot(snpName, snpPath == null ? null : snpPath.getAbsolutePath());

                for (int incIdx = 1; incIdx < 3; incIdx++) {
                    addData(cli);

                    if (snpPath == null)
                        snpCreate.createIncrementalSnapshot(snpName).get(TIMEOUT);
                    else
                        snpCreate.createSnapshot(snpName, snpPath.getAbsolutePath(), true, false).get(TIMEOUT);

                    for (int gridIdx = 0; gridIdx < GRID_CNT; gridIdx++) {
                        assertTrue("Incremental snapshot must exists on node " + gridIdx, checkIncremental(
                            grid(gridIdx),
                            snpName,
                            snpPath == null ? null : snpPath.getAbsolutePath(),
                            incIdx
                        ));
                    }
                }
            }
        }
    }

    /** */
    private void addData(IgniteEx node) {
        IgniteCache<Integer, byte[]> cache = node.getOrCreateCache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < (int)(U.MB * 3 * GRID_CNT / U.KB); i++) {
            byte[] bytes = new byte[(int)U.KB];

            ThreadLocalRandom.current().nextBytes(bytes);

            cache.put(cntr.incrementAndGet(), bytes);
        }
    }

    /** */
    @Test
    public void testFailForUnknownBaseSnapshot() throws Exception {
        assumeFalse("https://issues.apache.org/jira/browse/IGNITE-17819", encryption);

        IgniteEx ign = startGridsWithCache(1, CACHE_KEYS_RANGE, key -> new Account(key, key),
            new CacheConfiguration<>(DEFAULT_CACHE_NAME));

        assertThrowsWithCause(
            () -> snp(ign).createIncrementalSnapshot("unknown").get(TIMEOUT),
            IgniteException.class
        );

        createAndCheckSnapshot(ign, SNAPSHOT_NAME, null, TIMEOUT);

        assertThrowsWithCause(
            () -> snp(ign).createIncrementalSnapshot("unknown").get(TIMEOUT),
            IgniteException.class
        );
    }

    /** */
    @Test
    public void testFailIfPreviousIncrementNotAvailable() throws Exception {
        assumeFalse("https://issues.apache.org/jira/browse/IGNITE-17819", encryption);

        IgniteEx srv = startGridsWithCache(
            GRID_CNT,
            CACHE_KEYS_RANGE,
            key -> new Account(key, key),
            new CacheConfiguration<>(DEFAULT_CACHE_NAME)
        );

        IgniteEx cli = startClientGrid(
            GRID_CNT,
            (UnaryOperator<IgniteConfiguration>)
                cfg -> cfg.setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME))
        );

        createAndCheckSnapshot(cli, SNAPSHOT_NAME, null, TIMEOUT);

        cli.snapshot().createIncrementalSnapshot(SNAPSHOT_NAME).get(TIMEOUT);
        cli.snapshot().createIncrementalSnapshot(SNAPSHOT_NAME).get(TIMEOUT);

        File toRmv = new File(
            snp(ignite(GRID_CNT - 1)).incrementalSnapshotLocalDir(SNAPSHOT_NAME, null, 2),
            snapshotMetaFileName(ignite(GRID_CNT - 1).localNode().consistentId().toString()));

        assertTrue(toRmv.exists());

        U.delete(toRmv);

        assertThrowsWithCause(
            () -> cli.snapshot().createIncrementalSnapshot(SNAPSHOT_NAME).get(TIMEOUT),
            IgniteException.class
        );
    }

    /** */
    @Test
    public void testFailIfSegmentNotFound() throws Exception {
        assumeFalse("https://issues.apache.org/jira/browse/IGNITE-17819", encryption);

        IgniteEx srv = startGridsWithCache(
            1,
            CACHE_KEYS_RANGE,
            key -> new Account(key, key),
            new CacheConfiguration<>(DEFAULT_CACHE_NAME)
        );

        createAndCheckSnapshot(srv, SNAPSHOT_NAME, null, TIMEOUT);

        addData(srv);

        FileWriteAheadLogManager wal = (FileWriteAheadLogManager)srv.context().cache().context().wal();

        assertTrue(waitForCondition(() -> wal.lastCompactedSegment() >= 0, getTestTimeout()));

        long segIdx = wal.lastCompactedSegment();

        U.delete(wal.compactedSegment(segIdx));

        assertThrowsWithCause(
            () -> srv.snapshot().createIncrementalSnapshot(SNAPSHOT_NAME).get(TIMEOUT),
            IgniteException.class
        );
    }

    /** */
    @Test
    public void testIncrementalSnapshotFailsOnTopologyChange() throws Exception {
        assumeFalse("https://issues.apache.org/jira/browse/IGNITE-17819", encryption);

        IgniteEx srv = startGridsWithCache(
            GRID_CNT,
            CACHE_KEYS_RANGE,
            key -> new Account(key, key),
            new CacheConfiguration<>(DEFAULT_CACHE_NAME)
        );

        IgniteSnapshotManager snpCreate = snp(srv);

        createAndCheckSnapshot(srv, SNAPSHOT_NAME, null, TIMEOUT);

        String consId = grid(1).context().discovery().localNode().consistentId().toString();

        // Stop some node.
        stopGrid(1);

        assertThrows(
            null,
            () -> snpCreate.createIncrementalSnapshot(SNAPSHOT_NAME).get(TIMEOUT),
            IgniteException.class,
            "Create incremental snapshot request has been rejected. " +
                "Node from full snapshot offline [consistentId=" + consId + ']'
        );
    }

    /** */
    @Test
    public void testIncrementalSnapshotFailsOnCacheDestroy() throws Exception {
        assumeFalse("https://issues.apache.org/jira/browse/IGNITE-17819", encryption);

        checkFailWhenCacheDestroyed(OTHER_CACHE, "Create incremental snapshot request has been rejected. " +
            "Cache group destroyed [groupId=" + CU.cacheId(OTHER_CACHE) + ']');
    }

    /** */
    @Test
    public void testIncrementalSnapshotFailsOnGroupedCacheDestroy() throws Exception {
        assumeFalse("https://issues.apache.org/jira/browse/IGNITE-17819", encryption);

        checkFailWhenCacheDestroyed(GROUPED_CACHE, "Create incremental snapshot request has been rejected. " +
            "Cache destroyed [cacheId=" + CU.cacheId(GROUPED_CACHE) + ", cacheName=" + GROUPED_CACHE + ']');
    }

    /** */
    @Test
    public void testIncrementalSnapshotFailsOnCacheChange() throws Exception {
        assumeFalse("https://issues.apache.org/jira/browse/IGNITE-17819", encryption);

        CacheConfiguration<Integer, Account> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        IgniteEx srv = startGridsWithCache(1, CACHE_KEYS_RANGE, key -> new Account(key, key), ccfg);

        createAndCheckSnapshot(srv, SNAPSHOT_NAME, null, TIMEOUT);

        GridLocalConfigManager locCfgMgr = srv.context().cache().configManager();

        File ccfgFile = locCfgMgr.cacheConfigurationFile(ccfg);

        StoredCacheData cacheData = locCfgMgr.readCacheData(ccfgFile);

        assertNotNull(cacheData);

        cacheData.queryEntities(singletonList(new QueryEntity(String.class, Account.class)));

        locCfgMgr.writeCacheData(cacheData, ccfgFile);

        assertThrows(
            null,
            () -> snp(srv).createIncrementalSnapshot(SNAPSHOT_NAME).get(TIMEOUT),
            IgniteException.class,
            "Cache changed [cacheId=" + CU.cacheId(DEFAULT_CACHE_NAME) + ", cacheName=" + DEFAULT_CACHE_NAME + ']'
        );
    }

    /** */
    @Test
    public void testIncrementalSnapshotFailOnDirtyDir() throws Exception {
        assumeFalse("https://issues.apache.org/jira/browse/IGNITE-17819", encryption);

        IgniteEx srv = startGridsWithCache(
            GRID_CNT,
            CACHE_KEYS_RANGE,
            key -> new Account(key, key),
            new CacheConfiguration<>(DEFAULT_CACHE_NAME)
        );

        createAndCheckSnapshot(srv, SNAPSHOT_NAME, null, TIMEOUT);

        assertTrue(snp(srv).incrementalSnapshotsLocalRootDir(SNAPSHOT_NAME, null).mkdirs());
        assertTrue(snp(srv).incrementalSnapshotLocalDir(SNAPSHOT_NAME, null, 1).createNewFile());

        assertThrows(
            null,
            () -> snp(srv).createIncrementalSnapshot(SNAPSHOT_NAME).get(TIMEOUT),
            IgniteException.class,
            "Failed to create snapshot WAL directory"
        );

        for (int i = 0; i < GRID_CNT; i++)
            assertFalse(snp(grid(i)).incrementalSnapshotLocalDir(SNAPSHOT_NAME, null, 1).exists());
    }

    /** */
    @Test
    public void testFailIfWalCompactionDisabled() throws Exception {
        assumeFalse("https://issues.apache.org/jira/browse/IGNITE-17819", encryption);

        walCompactionEnabled = false;

        try {
            IgniteEx srv = startGridsWithCache(
                1,
                CACHE_KEYS_RANGE,
                key -> new Account(key, key),
                new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            );

            createAndCheckSnapshot(srv, SNAPSHOT_NAME, null, TIMEOUT);

            assertThrows(
                null,
                () -> srv.snapshot().createIncrementalSnapshot(SNAPSHOT_NAME).get(TIMEOUT),
                IgniteException.class,
                "Create incremental snapshot request has been rejected. WAL compaction must be enabled."
            );
        }
        finally {
            walCompactionEnabled = true;
        }

    }

    /** @throws Exception if failed. */
    @Test
    public void testStagesFail() throws Exception {
        assumeFalse("https://issues.apache.org/jira/browse/IGNITE-17819", encryption);

        DistributedProcess.DistributedProcessType[] stages = new DistributedProcess.DistributedProcessType[] {
            RESTORE_CACHE_GROUP_SNAPSHOT_PREPARE,
            RESTORE_CACHE_GROUP_SNAPSHOT_PRELOAD,
            RESTORE_INCREMENTAL_SNAPSHOT_START
        };

        IgniteEx srv = startGridsWithCache(
            2,
            CACHE_KEYS_RANGE,
            key -> new Account(key, key),
            new CacheConfiguration<>(DEFAULT_CACHE_NAME)
        );

        createAndCheckSnapshot(srv, SNAPSHOT_NAME, null, TIMEOUT);
        srv.snapshot().createIncrementalSnapshot(SNAPSHOT_NAME).get(TIMEOUT);

        AtomicReference<DistributedProcess.DistributedProcessType> failStage = new AtomicReference<>();

        TestRecordingCommunicationSpi.spi(grid(1)).blockMessages((node, msg) -> {
            if (msg instanceof SingleNodeMessage) {
                SingleNodeMessage<?> singleMsg = (SingleNodeMessage<?>)msg;

                if (failStage.get().ordinal() == singleMsg.type())
                    GridTestUtils.setFieldValue(singleMsg, "err", new IgniteException("Test exception."));
            }

            return false;
        });

        srv.destroyCache(DEFAULT_CACHE_NAME);

        awaitPartitionMapExchange();

        for (DistributedProcess.DistributedProcessType stage : stages) {
            failStage.set(stage);

            assertThrows(log,
                () -> srv.snapshot().restoreSnapshot(SNAPSHOT_NAME, singleton(DEFAULT_CACHE_NAME), 1).get(),
                IgniteException.class, "Test exception.");
        }
    }

    /** */
    private void checkFailWhenCacheDestroyed(String cache2rvm, String errMsg) throws Exception {
        IgniteEx srv = startGridsWithCache(
            1,
            CACHE_KEYS_RANGE,
            key -> new Account(key, key),
            new CacheConfiguration<>(DEFAULT_CACHE_NAME),
            new CacheConfiguration<>(OTHER_CACHE),
            new CacheConfiguration<Integer, Object>("my-grouped-cache1").setGroupName("mygroup"),
            new CacheConfiguration<Integer, Object>(GROUPED_CACHE).setGroupName("mygroup")
        );

        IgniteSnapshotManager snpCreate = snp(srv);

        createAndCheckSnapshot(srv, SNAPSHOT_NAME, null, TIMEOUT);

        snpCreate.createIncrementalSnapshot(SNAPSHOT_NAME).get(TIMEOUT);

        srv.destroyCache(cache2rvm);

        assertThrows(
            null,
            () -> snpCreate.createIncrementalSnapshot(SNAPSHOT_NAME).get(TIMEOUT),
            IgniteException.class,
            errMsg
        );
    }
}
