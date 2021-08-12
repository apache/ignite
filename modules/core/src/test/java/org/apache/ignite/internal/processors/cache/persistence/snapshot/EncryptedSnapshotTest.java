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
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.encryption.AbstractEncryptionTest;
import org.apache.ignite.internal.util.distributed.FullMessage;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;
import org.junit.runners.Parameterized;

/**
 * Snapshot test for encrypted snapshots.
 */
public class EncryptedSnapshotTest extends AbstractSnapshotSelfTest {
    /** Parameters. */
    @Parameterized.Parameters(name = "Encryption is enabled.")
    public static Iterable<Boolean> enableEncryption() {
        return Collections.singletonList(true);
    }

    /** Name of the cache being created in runtime. */
    private static final String RUNTIME_CACHE_NAME = "runtimeCache";

    /** {@inheritDoc} */
    @Override protected Function<Integer, Object> valueBuilder() {
        return (i -> new Account(i, i));
    }

    /** Checks creation of encrypted cache with same name after putting plain cache in snapshot. */
    @Test
    public void testEncryptedCacheCreatedAfterPlainCacheSnapshoting() throws Exception {
        testCacheCreatedAfterSnaphoting(true);
    }

    /** Checks creation of plain cache with same name after putting encrypted cache in snapshot. */
    @Test
    public void testPlainCacheCreatedAfterEncryptedCacheSnapshoting() throws Exception {
        testCacheCreatedAfterSnaphoting(false);
    }

    /**
     * Ensures that same-name-cache is created after putting cache into snapshot and deleting.
     *
     * @param encryptedFirst If {@code true}, creates encrypted cache before snapshoting and deleting. In reverse order if {@code
     *                             false}.
     */
    private void testCacheCreatedAfterSnaphoting(boolean encryptedFirst) throws Exception {
        startGrids(3);

        grid(0).cluster().state(ClusterState.ACTIVE);

        addCache(encryptedFirst);

        grid(1).snapshot().createSnapshot(SNAPSHOT_NAME).get(TIMEOUT);

        awaitPartitionMapExchange();

        grid(2).destroyCache(RUNTIME_CACHE_NAME);

        awaitPartitionMapExchange();

        addCache(!encryptedFirst);
    }

    /** Checks re-encryption fails during snapshot restoration. */
    @Test
    public void testReencryptDuringRestore() throws Exception {
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
    public void testReencryptDuringSnapshot() throws Exception {
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

    /** Checks snapshot restoration fails if different master key is contained in the snapshot. */
    @Test
    public void testStartFromSnapshotFailedWithOtherMasterKey() throws Exception {
        IgniteEx ig = startGridsWithCache(1, CACHE_KEYS_RANGE, valueBuilder(), dfltCacheCfg);

        ig.snapshot().createSnapshot(SNAPSHOT_NAME).get();

        ig.destroyCache(dfltCacheCfg.getName());

        ensureCacheAbsent(dfltCacheCfg);

        stopAllGrids(false);

        masterKeyName = AbstractEncryptionTest.MASTER_KEY_NAME_2;

        GridTestUtils.assertThrowsAnyCause(
            log,
            () -> startGridsFromSnapshot(1, SNAPSHOT_NAME),
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
            "Metastore is required because it contains encryption keys");
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
        startGridsWithCache(3, CACHE_KEYS_RANGE, valueBuilder(), dfltCacheCfg);

        BlockingCustomMessageDiscoverySpi grid0Disco = discoSpi(grid(0));

        IgniteFuture<Void> fut;

        if (restore) {
            CacheConfiguration<?, ?> notEncrCacheCfg = addCache(false);

            grid(1).snapshot().createSnapshot(SNAPSHOT_NAME).get(TIMEOUT);

            grid(2).cache(notEncrCacheCfg.getName()).destroy();

            awaitPartitionMapExchange();

            ensureCacheAbsent(notEncrCacheCfg);

            grid0Disco.block((msg) -> msg instanceof FullMessage && ((FullMessage)msg).error().isEmpty());

            fut = grid(1).snapshot().restoreSnapshot(SNAPSHOT_NAME, Collections.singletonList(notEncrCacheCfg.getName()));
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
        startGridsWithCache(3, CACHE_KEYS_RANGE, valueBuilder(), dfltCacheCfg);

        CacheConfiguration<?, ?> notEncrCacheCfg = addCache(false);

        grid(1).snapshot().createSnapshot(SNAPSHOT_NAME).get(TIMEOUT);

        grid(2).destroyCache(notEncrCacheCfg.getName());

        awaitPartitionMapExchange();

        ensureCacheAbsent(notEncrCacheCfg);

        BlockingCustomMessageDiscoverySpi discoSpi = discoSpi(grid(0));

        discoSpi.block(msg -> msg instanceof FullMessage && ((FullMessage)msg).error().isEmpty());

        IgniteFuture<?> fut = reencryption.apply(1);

        discoSpi.waitBlocked(TIMEOUT);

        GridTestUtils.assertThrowsAnyCause(log,
            () -> grid(2).snapshot().restoreSnapshot(SNAPSHOT_NAME, Collections.singletonList(RUNTIME_CACHE_NAME)).get(TIMEOUT),
            IgniteCheckedException.class,
            "Cache group restore operation was rejected. Master key changing or caches re-encryption process is not finished yet");

        GridTestUtils.assertThrowsAnyCause(log,
            () -> grid(2).snapshot().createSnapshot(SNAPSHOT_NAME + "_v2").get(TIMEOUT), IgniteCheckedException.class,
            "Snapshot operation has been rejected. Master key changing or caches re-encryption process is not finished yet");

        discoSpi.unblock();

        fut.get(TIMEOUT);
    }

    /**
     * Adds cache to the grid. Fills it and waits for PME.
     *
     * @param encrypted If {@code true}, created encrypted cache.
     * @return CacheConfiguration of the created cache.
     */
    private CacheConfiguration<?, ?> addCache(boolean encrypted) throws InterruptedException {
        CacheConfiguration<?, ?> cacheCfg = new CacheConfiguration<>(dfltCacheCfg).setName(RUNTIME_CACHE_NAME).
            setEncryptionEnabled(encrypted);

        grid(0).createCache(cacheCfg);

        Function<Integer, Object> valBuilder = valueBuilder();

        IgniteDataStreamer<Integer, Object> streamer = grid(0).dataStreamer(RUNTIME_CACHE_NAME);

        for (int i = 0; i < CACHE_KEYS_RANGE; i++)
            streamer.addData(i, valBuilder.apply(i));

        streamer.flush();

        awaitPartitionMapExchange();

        return cacheCfg;
    }

    /**
     * @return Cache group key change action.
     */
    private IgniteFuture<?> chageCacheGroupKey(int gridNum) {
        return grid(gridNum).encryption().changeCacheGroupKey(Collections.singletonList(dfltCacheCfg.getName()));
    }

    /**
     * @return Master key change action.
     */
    private IgniteFuture<?> chageMasterKey(int gridNum) {
        return grid(gridNum).encryption().changeMasterKey(AbstractEncryptionTest.MASTER_KEY_NAME_2);
    }
}
