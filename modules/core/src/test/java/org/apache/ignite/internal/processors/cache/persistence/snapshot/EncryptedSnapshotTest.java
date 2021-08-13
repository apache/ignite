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

import java.util.Collections;
import java.util.function.Function;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.encryption.AbstractEncryptionTest;
import org.apache.ignite.internal.processors.cache.verify.IdleVerifyResultV2;
import org.apache.ignite.internal.util.distributed.FullMessage;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;

/**
 * Snapshot test for encrypted-only snapshots.
 */
public class EncryptedSnapshotTest extends AbstractSnapshotSelfTest {
    /** Parameters. */
    @Parameterized.Parameters(name = "Encryption is enabled.")
    public static Iterable<Boolean> enableEncryption() {
        return Collections.singletonList(true);
    }

    /** Name of additional encrypted cache. */
    private static final String SECOND_CACHE_NAME = "cache2";

    /** {@inheritDoc} */
    @Override protected Function<Integer, Object> valueBuilder() {
        return (i -> new Account(i, i));
    }

    /** Checks both encrypted and plain caches can be restored from same snapshot. */
    @Test
    public void testRestoringEncryptedAndPlainCaches() throws Exception {
        start3GridsCreateEncrPlainSnp();

        grid(1).snapshot().restoreSnapshot(SNAPSHOT_NAME, null).get(TIMEOUT);

        assertCacheKeys(grid(2).cache(DEFAULT_CACHE_NAME), CACHE_KEYS_RANGE);
        assertCacheKeys(grid(2).cache(SECOND_CACHE_NAME), CACHE_KEYS_RANGE);
    }

    /** Checks both encrypted and plain caches can be restored from same snapshot. */
    @Test
    public void testStartingWithEncryptedAndPlainCaches() throws Exception {
        start3GridsCreateEncrPlainSnp();

        stopAllGrids();

        IgniteEx ig = startGridsFromSnapshot(3, SNAPSHOT_NAME);

        assertCacheKeys(ig.cache(DEFAULT_CACHE_NAME), CACHE_KEYS_RANGE);
        assertCacheKeys(ig.cache(SECOND_CACHE_NAME), CACHE_KEYS_RANGE);
    }

    /** Checks snapshot after single reencryption. */
    @Test
    public void testSnapshotRestoringAfterSingleReencryption() throws Exception {
        checkSnapshotWithReencryptedCache(1);
    }

    /** Checks snapshot after multiple reencryption. */
    @Test
    public void testSnapshotRestoringAfterMultipleReencryption() throws Exception {
        checkSnapshotWithReencryptedCache(3);
    }

    /** Checks snapshot validati fails if different master key is used. */
    @Test
    public void testSnapshotFailsWithOtherMasterKey() throws Exception {
        IgniteEx ig = startGridsWithCache(1, CACHE_KEYS_RANGE, valueBuilder(), dfltCacheCfg);

        ig.snapshot().createSnapshot(SNAPSHOT_NAME).get();

        ig.destroyCache(dfltCacheCfg.getName());

        ensureCacheAbsent(dfltCacheCfg);

        stopAllGrids(false);

        masterKeyName = AbstractEncryptionTest.MASTER_KEY_NAME_2;

        ig = startGrids(1);

        IdleVerifyResultV2 snpCheckRes = snp(ig).checkSnapshot(SNAPSHOT_NAME).get();

        for (Exception e : snpCheckRes.exceptions().values()) {
            if (e.getMessage().contains("different signature of the master key"))
                return;
        }

        throw new IllegalStateException("Snapshot validation must contain error due to different master key.");
    }

    /** Checks re-encryption fails during snapshot restoration. */
    @Test
    public void testReencryptionDuringRestore() throws Exception {
        checkActionFailsDuringSnapshotOperation(true, this::chageCacheGroupKey, "Cache group key change was " +
            "rejected.", IgniteException.class);
    }

    /** Checks master key changing failes during snapshot restoration. */
    @Test
    public void testMasterKeyChangeDuringRestore() throws Exception {
        checkActionFailsDuringSnapshotOperation(true, this::chageMasterKey, "Master key change was rejected.",
            IgniteException.class);
    }

    /** Checks re-encryption fails during snapshot creation. */
    @Test
    public void testReencryptionDuringSnapshot() throws Exception {
        checkActionFailsDuringSnapshotOperation(false, this::chageCacheGroupKey, "Cache group key change was " +
            "rejected.", IgniteException.class);
    }

    /** Checks master key changing fails during snapshot creation. */
    @Test
    public void testMasterKeyChangeDuringSnapshot() throws Exception {
        checkActionFailsDuringSnapshotOperation(false, this::chageMasterKey, "Master key change was rejected.",
            IgniteException.class);
    }

    /** Checks snapshot action fail during cache group key change. */
    @Test
    public void testSnapshotFailsDuringCacheKeyChange() throws Exception {
        checkSnapshotActionFailsDuringReencryption(this::chageCacheGroupKey);
    }

    /** Checks snapshot action fail during master key cnahge. */
    @Test
    public void testSnapshotFailsDuringMasterKeyChange() throws Exception {
        checkSnapshotActionFailsDuringReencryption(this::chageMasterKey);
    }

    /** Checks snapshot restoration fails if different master key is used. */
    @Test
    public void testSnapshotRestoringFailsWithOtherMasterKey() throws Exception {
        IgniteEx ig = startGridsWithCache(1, CACHE_KEYS_RANGE, valueBuilder(), dfltCacheCfg);

        snp(ig).createSnapshot(SNAPSHOT_NAME).get();

        ig.destroyCache(dfltCacheCfg.getName());

        ensureCacheAbsent(dfltCacheCfg);

        stopAllGrids(false);

        masterKeyName = AbstractEncryptionTest.MASTER_KEY_NAME_2;

        final IgniteEx ig1 = startGrids(1);

        ig1.cluster().state(ACTIVE);

        GridTestUtils.assertThrowsAnyCause(
            log,
            () -> snp(ig1).restoreSnapshot(SNAPSHOT_NAME, Collections.singletonList(dfltCacheCfg.getName())).get(TIMEOUT),
            IgniteCheckedException.class,
            "different signature of the master key"
        );
    }

    /** Checks snapshot restoration fails if different master key is contained in the snapshot. */
    @Test
    public void testStartingFromSnapshotFailsWithOtherMasterKey() throws Exception {
        IgniteEx ig = startGridsWithCache(3, CACHE_KEYS_RANGE, valueBuilder(), dfltCacheCfg);

        ig.snapshot().createSnapshot(SNAPSHOT_NAME).get();

        ig.destroyCache(dfltCacheCfg.getName());

        awaitPartitionMapExchange();

        ensureCacheAbsent(dfltCacheCfg);

        stopAllGrids(false);

        masterKeyName = AbstractEncryptionTest.MASTER_KEY_NAME_2;

        GridTestUtils.assertThrowsAnyCause(
            log,
            () -> startGridsFromSnapshot(3, SNAPSHOT_NAME),
            IgniteCheckedException.class,
            "bad key is used during decryption"
        );
    }

    /** Checks it is unavailable to register snapshot task for encrypted caches witout metastore. */
    @Test
    public void testSnapshotTaskIsBlockedWithoutMetastore() throws Exception {
        // Start grid node with data before each test.
        IgniteEx ig = startGridsWithCache(1, CACHE_KEYS_RANGE, valueBuilder(), dfltCacheCfg);

        GridTestUtils.assertThrowsAnyCause(log,
            () -> snp(ig).registerSnapshotTask(SNAPSHOT_NAME, ig.localNode().id(), F.asMap(CU.cacheId(dfltCacheCfg.getName()), null),
                false, snp(ig).localSnapshotSenderFactory().apply(SNAPSHOT_NAME)).get(TIMEOUT),
            IgniteCheckedException.class,
            "Metastore is requird because it holds encryption keys");
    }

    /**
     * Checks snapshot after reencryption.
     *
     * @param reencryptionIterations Number re-encryptions turns.
     */
    private void checkSnapshotWithReencryptedCache(int reencryptionIterations) throws Exception {
        int gridCnt = 3;

        IgniteEx ig = startGridsWithCache(gridCnt, CACHE_KEYS_RANGE, valueBuilder(), dfltCacheCfg.setName(SECOND_CACHE_NAME));

        for (int r = 0; r < reencryptionIterations; ++r) {
            chageCacheGroupKey(0).get(TIMEOUT);

            for (int g = 0; g < gridCnt; ++g)
                grid(g).context().encryption().reencryptionFuture(CU.cacheId(dfltCacheCfg.getName())).get();
        }

        ig.snapshot().createSnapshot(SNAPSHOT_NAME).get(TIMEOUT);

        ig.cache(dfltCacheCfg.getName()).destroy();

        awaitPartitionMapExchange();

        ensureCacheAbsent(dfltCacheCfg);

        awaitPartitionMapExchange();

        ig.snapshot().restoreSnapshot(SNAPSHOT_NAME, null).get(TIMEOUT);

        assertCacheKeys(grid(1).cache(dfltCacheCfg.getName()), CACHE_KEYS_RANGE);

        stopAllGrids();

        startGridsFromSnapshot(gridCnt, SNAPSHOT_NAME);

        assertCacheKeys(grid(1).cache(dfltCacheCfg.getName()), CACHE_KEYS_RANGE);
    }

    /**
     * Checks {@code action} is blocked with {@code errPrefix} and {@code errEncrypType} during active snapshot.
     *
     * @param restore If {@code true}, snapshot restoration is activated during the test. Snapshot creation otherwise.
     * @param action Action to call during snapshot operation. Its param is the grid num.
     * @param errPrefix Prefix of error message text to search for.
     * @param errType Type of exception to search for.
     */
    private void checkActionFailsDuringSnapshotOperation(boolean restore, Function<Integer, IgniteFuture<?>> action, String errPrefix,
        Class<? extends Exception> errType) throws Exception {
        startGridsWithCache(3, CACHE_KEYS_RANGE, valueBuilder(), dfltCacheCfg,
            new CacheConfiguration<>(dfltCacheCfg).setName(SECOND_CACHE_NAME));

        BlockingCustomMessageDiscoverySpi grid0Disco = discoSpi(grid(0));

        IgniteFuture<Void> fut;

        if (restore) {
            grid(1).snapshot().createSnapshot(SNAPSHOT_NAME).get(TIMEOUT);

            grid(2).cache(dfltCacheCfg.getName()).destroy();

            awaitPartitionMapExchange();

            ensureCacheAbsent(dfltCacheCfg);

            grid0Disco.block((msg) -> msg instanceof FullMessage && ((FullMessage)msg).error().isEmpty());

            fut = grid(1).snapshot().restoreSnapshot(SNAPSHOT_NAME, Collections.singletonList(dfltCacheCfg.getName()));
        }
        else {
            grid0Disco.block((msg) -> msg instanceof FullMessage && ((FullMessage)msg).error().isEmpty());

            fut = grid(1).snapshot().createSnapshot(SNAPSHOT_NAME);
        }

        grid0Disco.waitBlocked(TIMEOUT);

        GridTestUtils.assertThrowsAnyCause(log, () -> action.apply(2).get(TIMEOUT), errType,
            errPrefix + " Snapshot operation is in progress.");

        grid0Disco.unblock();

        fut.get(TIMEOUT);
    }

    /**
     * Checks snapshot action is blocked during {@code reencryption}.
     *
     * @param reencryption Any kind of re-encryption action.
     */
    private void checkSnapshotActionFailsDuringReencryption(Function<Integer, IgniteFuture<?>> reencryption) throws Exception {
        startGridsWithCache(3, CACHE_KEYS_RANGE, valueBuilder(), dfltCacheCfg,
            new CacheConfiguration<>(dfltCacheCfg).setName(SECOND_CACHE_NAME));

        snp(grid(1)).createSnapshot(SNAPSHOT_NAME).get(TIMEOUT);

        grid(1).destroyCache(dfltCacheCfg.getName());

        awaitPartitionMapExchange();

        ensureCacheAbsent(dfltCacheCfg);

        BlockingCustomMessageDiscoverySpi discoSpi = discoSpi(grid(0));

        discoSpi.block(msg -> msg instanceof FullMessage && ((FullMessage)msg).error().isEmpty());

        IgniteFuture<?> fut = reencryption.apply(1);

        discoSpi.waitBlocked(TIMEOUT);

        GridTestUtils.assertThrowsAnyCause(log,
            () -> grid(2).snapshot().restoreSnapshot(SNAPSHOT_NAME, Collections.singletonList(dfltCacheCfg.getName())).get(TIMEOUT),
            IgniteCheckedException.class,
            "Cache group restore operation was rejected. Master key changing or caches re-encryption process is not finished yet");

        GridTestUtils.assertThrowsAnyCause(log,
            () -> grid(2).snapshot().createSnapshot(SNAPSHOT_NAME + "_v2").get(TIMEOUT), IgniteCheckedException.class,
            "Snapshot operation has been rejected. Master key changing or caches re-encryption process is not finished yet");

        discoSpi.unblock();

        fut.get(TIMEOUT);
    }

    /**
     * Adds not-encrypted cache named {@link #SECOND_CACHE_NAME} and fills it with {@link #CACHE_KEYS_RANGE} records.
     *
     * @return CacheConfiguration of the new cache.
     */
    private CacheConfiguration<Integer, Object> addNotEncryptedCache(IgniteEx ig) {
        CacheConfiguration<Integer, Object> ccfg =
            new CacheConfiguration<>(dfltCacheCfg).setName(SECOND_CACHE_NAME).setEncryptionEnabled(false);

        ig.createCache(ccfg);

        Function<Integer, Object> valBuilder = valueBuilder();

        try (IgniteDataStreamer<Integer, Object> ds = ig.dataStreamer(SECOND_CACHE_NAME)) {
            for (int i = 0; i < CACHE_KEYS_RANGE; ++i)
                ds.addData(i, valBuilder.apply(i));
        }

        return ccfg;
    }

    /**
     * Starts 3 nodes, creates encrypted and plain caches, creates snapshot, destroes the caches.
     */
    private void start3GridsCreateEncrPlainSnp() throws Exception {
        startGridsWithCache(3, CACHE_KEYS_RANGE, valueBuilder(), dfltCacheCfg);

        CacheConfiguration<Integer, Object> ccfg = addNotEncryptedCache(grid(0));

        grid(1).snapshot().createSnapshot(SNAPSHOT_NAME).get(TIMEOUT);

        grid(2).cache(DEFAULT_CACHE_NAME).destroy();
        grid(0).cache(SECOND_CACHE_NAME).destroy();

        awaitPartitionMapExchange();

        ensureCacheAbsent(dfltCacheCfg);
        ensureCacheAbsent(ccfg);
    }

    /**
     * @return Cache group key change action.
     */
    private IgniteFuture<?> chageCacheGroupKey(int gridNum) {
        return grid(gridNum).encryption().changeCacheGroupKey(Collections.singletonList(SECOND_CACHE_NAME));
    }

    /**
     * @return Master key change action.
     */
    private IgniteFuture<?> chageMasterKey(int gridNum) {
        return grid(gridNum).encryption().changeMasterKey(AbstractEncryptionTest.MASTER_KEY_NAME_2);
    }
}
