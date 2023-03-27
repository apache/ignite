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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.encryption.AbstractEncryptionTest;
import org.apache.ignite.internal.processors.cache.verify.IdleVerifyResultV2;
import org.apache.ignite.internal.util.distributed.FullMessage;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.configuration.IgniteConfiguration.DFLT_SNAPSHOT_DIRECTORY;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.CACHE_GROUP_KEY_CHANGE_PREPARE;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.MASTER_KEY_CHANGE_PREPARE;

/**
 * Snapshot test for encrypted-only snapshots.
 */

public class EncryptedSnapshotTest extends AbstractSnapshotSelfTest {
    /** Second cache name. */
    private static final String CACHE2 = "cache2";

    /** Parameters. */
    @Parameterized.Parameters(name = "encryption={0}, onlyPrimay={1}")
    public static List<Object[]> disableEncryption() {
        return Arrays.asList(
            new Object[]{true, false},
            new Object[]{true, true}
        );
    }

    /** {@inheritDoc} */
    @Override protected Function<Integer, Object> valueBuilder() {
        return (i -> new Account(i, i));
    }

    /** Checks creation of encrypted cache with same name after putting plain cache in snapshot. */
    @Test
    public void testEncryptedCacheCreatedAfterPlainCacheSnapshotting() throws Exception {
        testCacheCreatedAfterSnapshotting(true);
    }

    /** Checks creation of plain cache with same name after putting encrypted cache in snapshot. */
    @Test
    public void testPlainCacheCreatedAfterEncryptedCacheSnapshotting() throws Exception {
        testCacheCreatedAfterSnapshotting(false);
    }

    /** Checks re-encryption fails during snapshot restoration. */
    @Test
    public void testReencryptDuringRestore() throws Exception {
        checkActionFailsDuringSnapshotOperation(true, this::changeCacheKey, "Cache group key change " +
            "was rejected.", IgniteException.class);
    }

    /** Checks master key changing fails during snapshot restoration. */
    @Test
    public void testMasterKeyChangeDuringRestore() throws Exception {
        checkActionFailsDuringSnapshotOperation(true, this::changeMasterKey, "Master key change was " +
            "rejected.", IgniteException.class);
    }

    /** Checks re-encryption fails during snapshot creation. */
    @Test
    public void testReencryptDuringSnapshot() throws Exception {
        checkActionFailsDuringSnapshotOperation(false, this::changeCacheKey, "Cache group key change " +
            "was rejected.", IgniteException.class);
    }

    /** Checks master key changing fails during snapshot creation. */
    @Test
    public void testMasterKeyChangeDuringSnapshot() throws Exception {
        checkActionFailsDuringSnapshotOperation(false, this::changeMasterKey, "Master key change was " +
            "rejected.", IgniteException.class);
    }

    /** Checks snapshot action fail during cache group key change. */
    @Test
    public void testSnapshotFailsDuringCacheKeyChange() throws Exception {
        checkSnapshotActionFailsDuringReencryption(this::changeCacheKey, "Caches re-encryption process " +
            "is not finished yet");
    }

    /** Checks snapshot action fail during master key change. */
    @Test
    public void testSnapshotFailsDuringMasterKeyChange() throws Exception {
        checkSnapshotActionFailsDuringReencryption(this::changeMasterKey, "Master key changing process " +
            "is not finished yet.");
    }

    /** Checks snapshot restoration fails if different master key is used. */
    @Test
    public void testSnapshotRestoringFailsWithOtherMasterKey() throws Exception {
        IgniteEx ig = startGridsWithCache(2, CACHE_KEYS_RANGE, valueBuilder(), dfltCacheCfg);

        createAndCheckSnapshot(ig, SNAPSHOT_NAME);

        ig.destroyCache(dfltCacheCfg.getName());

        ensureCacheAbsent(dfltCacheCfg);

        stopAllGrids(false);

        masterKeyName = AbstractEncryptionTest.MASTER_KEY_NAME_2;

        final IgniteEx ig1 = startGrids(2);

        ig1.cluster().state(ACTIVE);

        GridTestUtils.assertThrowsAnyCause(
            log,
            () -> snp(ig1).restoreSnapshot(SNAPSHOT_NAME, Collections.singletonList(dfltCacheCfg.getName())).get(TIMEOUT),
            IgniteCheckedException.class,
            "different master key digest"
        );
    }

    /** Checks both encrypted and plain caches can be restored from same snapshot. */
    @Test
    public void testRestoringEncryptedAndPlainCaches() throws Exception {
        start2GridsWithEncryptesAndPlainCachesSnapshot();

        grid(1).snapshot().restoreSnapshot(SNAPSHOT_NAME, null).get(TIMEOUT);

        assertCacheKeys(grid(1).cache(DEFAULT_CACHE_NAME), CACHE_KEYS_RANGE);
        assertCacheKeys(grid(1).cache(CACHE2), CACHE_KEYS_RANGE);
    }

    /** Checks both encrypted and plain caches can be restored from same snapshot. */
    @Test
    public void testStartingWithEncryptedAndPlainCaches() throws Exception {
        start2GridsWithEncryptesAndPlainCachesSnapshot();

        stopAllGrids();

        IgniteEx ig = startGridsFromSnapshot(2, SNAPSHOT_NAME);

        assertCacheKeys(ig.cache(DEFAULT_CACHE_NAME), CACHE_KEYS_RANGE);
        assertCacheKeys(ig.cache(CACHE2), CACHE_KEYS_RANGE);
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
    public void testValidatingSnapshotFailsWithOtherMasterKey() throws Exception {
        IgniteEx ig = startGridsWithCache(2, CACHE_KEYS_RANGE, valueBuilder(), dfltCacheCfg);

        createAndCheckSnapshot(ig, SNAPSHOT_NAME);

        ig.destroyCache(dfltCacheCfg.getName());

        ensureCacheAbsent(dfltCacheCfg);

        stopAllGrids(false);

        masterKeyName = AbstractEncryptionTest.MASTER_KEY_NAME_2;

        ig = startGrids(2);

        IdleVerifyResultV2 snpCheckRes = snp(ig).checkSnapshot(SNAPSHOT_NAME, null).get().idleVerifyResult();

        for (Exception e : snpCheckRes.exceptions().values()) {
            if (e.getMessage().contains("different master key digest"))
                return;
        }

        throw new IllegalStateException("Snapshot validation must contain error due to different master key.");
    }

    /** @throws Exception If fails. */
    @Test
    public void testValidatingSnapshotFailsWithNoEncryption() throws Exception {
        File tmpSnpDir = null;

        try {
            startGridsWithSnapshot(3, CACHE_KEYS_RANGE, false);

            stopAllGrids();

            encryption = false;
            dfltCacheCfg = null;

            File snpDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_SNAPSHOT_DIRECTORY, false);
            assertTrue(snpDir.isDirectory() && snpDir.listFiles().length > 0);

            tmpSnpDir = new File(snpDir.getAbsolutePath() + "_tmp");

            assertTrue(tmpSnpDir.length() == 0);

            assertTrue(snpDir.renameTo(tmpSnpDir));

            cleanPersistenceDir();

            assertTrue(tmpSnpDir.renameTo(snpDir));

            IgniteEx ig = startGrids(3);

            snpDir.renameTo(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_SNAPSHOT_DIRECTORY, false));

            ig.cluster().state(ACTIVE);

            IdleVerifyResultV2 snpCheckRes = snp(ig).checkSnapshot(SNAPSHOT_NAME, null).get().idleVerifyResult();

            for (Exception e : snpCheckRes.exceptions().values()) {
                if (e.getMessage().contains("has encrypted caches while encryption is disabled"))
                    return;
            }

            throw new IllegalStateException("Snapshot validation must contain error due to encryption is currently " +
                "disabled.");
        }
        finally {
            if (tmpSnpDir != null)
                U.delete(tmpSnpDir);
        }
    }

    /** Checks snapshot restoration fails if different master key is contained in the snapshot. */
    @Test
    public void testStartFromSnapshotFailedWithOtherMasterKey() throws Exception {
        IgniteEx ig = startGridsWithCache(2, CACHE_KEYS_RANGE, valueBuilder(), dfltCacheCfg);

        createAndCheckSnapshot(ig, SNAPSHOT_NAME);

        ig.destroyCache(dfltCacheCfg.getName());

        ensureCacheAbsent(dfltCacheCfg);

        stopAllGrids(false);

        masterKeyName = AbstractEncryptionTest.MASTER_KEY_NAME_2;

        GridTestUtils.assertThrowsAnyCause(
            log,
            () -> startGridsFromSnapshot(2, SNAPSHOT_NAME),
            IgniteSpiException.class,
            "bad key is used during decryption"
        );
    }

    /** Checks it is unavailable to register snapshot task for encrypted caches without metastore. */
    @Test
    public void testSnapshotTaskIsBlockedWithoutMetastore() throws Exception {
        // Start grid node with data before each test.
        IgniteEx ig = startGridsWithCache(1, CACHE_KEYS_RANGE, valueBuilder(), dfltCacheCfg);

        GridTestUtils.assertThrowsAnyCause(log,
            () -> snp(ig).registerSnapshotTask(SNAPSHOT_NAME, ig.localNode().id(),
                null, F.asMap(CU.cacheId(dfltCacheCfg.getName()), null), false,
                snp(ig).localSnapshotSenderFactory().apply(SNAPSHOT_NAME, null)).get(TIMEOUT),
            IgniteCheckedException.class,
            "Metastore is required because it holds encryption keys");
    }

    /** {@inheritDoc} */
    @Override protected void ensureCacheAbsent(
        CacheConfiguration<?, ?> ccfg) throws IgniteCheckedException, InterruptedException {
        awaitPartitionMapExchange();

        super.ensureCacheAbsent(ccfg);
    }

    /**
     * Ensures that same-name-cache is created after putting cache into snapshot and deleting.
     *
     * @param encryptedFirst If {@code true}, creates encrypted cache before snapshoting and deleting. In reverse order
     *                       {@code false}.
     */
    private void testCacheCreatedAfterSnapshotting(boolean encryptedFirst) throws Exception {
        startGrids(2);

        grid(0).cluster().state(ClusterState.ACTIVE);

        addCache(encryptedFirst);

        createAndCheckSnapshot(grid(1), SNAPSHOT_NAME, null, TIMEOUT);

        awaitPartitionMapExchange();

        grid(0).destroyCache(CACHE2);

        awaitPartitionMapExchange();

        addCache(!encryptedFirst);
    }

    /**
     * Checks snapshot after reencryption.
     *
     * @param reencryptionIterations Number re-encryptions turns.
     */
    private void checkSnapshotWithReencryptedCache(int reencryptionIterations) throws Exception {
        IgniteEx ig = startGridsWithCache(2, CACHE_KEYS_RANGE, valueBuilder(), dfltCacheCfg.setName(CACHE2));

        for (int r = 0; r < reencryptionIterations; ++r) {
            changeCacheKey(0).get(TIMEOUT);

            for (int g = 0; g < 2; ++g)
                grid(g).context().encryption().reencryptionFuture(CU.cacheId(dfltCacheCfg.getName())).get();
        }

        createAndCheckSnapshot(ig, SNAPSHOT_NAME, null, TIMEOUT);

        ig.cache(dfltCacheCfg.getName()).destroy();

        ensureCacheAbsent(dfltCacheCfg);

        ig.snapshot().restoreSnapshot(SNAPSHOT_NAME, null).get(TIMEOUT);

        assertCacheKeys(grid(1).cache(dfltCacheCfg.getName()), CACHE_KEYS_RANGE);

        stopAllGrids();

        startGridsFromSnapshot(2, SNAPSHOT_NAME);

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
    private void checkActionFailsDuringSnapshotOperation(boolean restore, Function<Integer, IgniteFuture<?>> action,
        String errPrefix, Class<? extends Exception> errType) throws Exception {
        startGridsWithCache(3, CACHE_KEYS_RANGE, valueBuilder(), dfltCacheCfg,
            new CacheConfiguration<>(dfltCacheCfg).setName(CACHE2));

        BlockingCustomMessageDiscoverySpi spi0 = discoSpi(grid(0));

        IgniteFuture<Void> fut;

        if (restore) {
            createAndCheckSnapshot(grid(1), SNAPSHOT_NAME, null, TIMEOUT);

            grid(1).cache(dfltCacheCfg.getName()).destroy();

            ensureCacheAbsent(dfltCacheCfg);

            spi0.block((msg) -> msg instanceof FullMessage && ((FullMessage<?>)msg).error().isEmpty());

            fut = grid(1).snapshot().restoreSnapshot(SNAPSHOT_NAME, Collections.singletonList(dfltCacheCfg.getName()));
        }
        else {
            spi0.block((msg) -> msg instanceof FullMessage && ((FullMessage<?>)msg).error().isEmpty());

            fut = snp(grid(1)).createSnapshot(SNAPSHOT_NAME, null, false, onlyPrimary);
        }

        spi0.waitBlocked(TIMEOUT);

        GridTestUtils.assertThrowsAnyCause(log, () -> action.apply(2).get(TIMEOUT), errType,
            errPrefix + " Snapshot operation is in progress.");

        spi0.unblock();

        fut.get(TIMEOUT);
    }

    /**
     * Checks snapshot action is blocked during {@code reencryption}.
     *
     * @param reencryption Any kind of re-encryption action.
     */
    private void checkSnapshotActionFailsDuringReencryption(Function<Integer, IgniteFuture<?>> reencryption,
        String expectedError) throws Exception {
        startGridsWithCache(3, CACHE_KEYS_RANGE, valueBuilder(), dfltCacheCfg,
            new CacheConfiguration<>(dfltCacheCfg).setName(CACHE2));

        // + non-baseline node.
        grid(0).cluster().baselineAutoAdjustEnabled(false);

        grid(0).cluster().setBaselineTopology(grid(0).cluster().topologyVersion());

        startGrid(G.allGrids().size());

        createAndCheckSnapshot(grid(1), SNAPSHOT_NAME, null, TIMEOUT);

        grid(2).destroyCache(dfltCacheCfg.getName());

        ensureCacheAbsent(dfltCacheCfg);

        BlockingCustomMessageDiscoverySpi discoSpi = discoSpi(grid(0));

        discoSpi.block(msg -> msg instanceof FullMessage &&
            (((FullMessage<?>)msg).type() == CACHE_GROUP_KEY_CHANGE_PREPARE.ordinal()
                || (((FullMessage<?>)msg).type() == MASTER_KEY_CHANGE_PREPARE.ordinal()) &&
                ((FullMessage<?>)msg).error().isEmpty()));

        IgniteFuture<?> fut = reencryption.apply(1);

        discoSpi.waitBlocked(TIMEOUT);

        GridTestUtils.assertThrowsAnyCause(log,
            () -> grid(1).snapshot().restoreSnapshot(SNAPSHOT_NAME, Collections.singletonList(CACHE2)).get(TIMEOUT),
            IgniteCheckedException.class,
            expectedError);

        GridTestUtils.assertThrowsAnyCause(log,
            () -> snp(grid(2)).createSnapshot(SNAPSHOT_NAME + "_v2", null, false, onlyPrimary).get(TIMEOUT), IgniteCheckedException.class,
            expectedError);

        discoSpi.unblock();

        fut.get(TIMEOUT);
    }

    /**
     * Adds cache to the grid. Fills it and waits for PME.
     *
     * @param encrypted If {@code true}, created encrypted cache.
     * @return CacheConfiguration of the created cache.
     */
    private CacheConfiguration<?, ?> addCache(boolean encrypted) throws IgniteCheckedException {
        CacheConfiguration<?, ?> cacheCfg = new CacheConfiguration<>(dfltCacheCfg).setName(CACHE2).
            setEncryptionEnabled(encrypted);

        grid(0).createCache(cacheCfg);

        Function<Integer, Object> valBuilder = valueBuilder();

        IgniteDataStreamer<Integer, Object> streamer = grid(0).dataStreamer(CACHE2);

        for (int i = 0; i < CACHE_KEYS_RANGE; i++)
            streamer.addData(i, valBuilder.apply(i));

        streamer.flush();

        forceCheckpoint();

        return cacheCfg;
    }

    /**
     * Starts 2 nodes, creates encrypted and plain caches, creates snapshot, destroes the caches. Ensures caches absent.
     */
    private void start2GridsWithEncryptesAndPlainCachesSnapshot() throws Exception {
        startGridsWithCache(2, CACHE_KEYS_RANGE, valueBuilder(), dfltCacheCfg);

        CacheConfiguration<?, ?> ccfg = addCache(false);

        createAndCheckSnapshot(grid(1), SNAPSHOT_NAME, null, TIMEOUT);

        grid(1).cache(DEFAULT_CACHE_NAME).destroy();
        grid(1).cache(CACHE2).destroy();

        ensureCacheAbsent(dfltCacheCfg);
        ensureCacheAbsent(ccfg);
    }

    /**
     * @return Cache group key change action.
     */
    private IgniteFuture<?> changeCacheKey(int gridNum) {
        return grid(gridNum).encryption().changeCacheGroupKey(Collections.singletonList(CACHE2));
    }

    /**
     * @return Master key change action.
     */
    private IgniteFuture<?> changeMasterKey(int gridNum) {
        return grid(gridNum).encryption().changeMasterKey(AbstractEncryptionTest.MASTER_KEY_NAME_2);
    }
}
