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
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.encryption.AbstractEncryptionTest;
import org.apache.ignite.internal.processors.cache.DynamicCacheChangeBatch;
import org.apache.ignite.internal.processors.cache.verify.IdleVerifyResultV2;
import org.apache.ignite.internal.util.distributed.FullMessage;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;

/**
 * Snapshot test only for encrypted-related cases.
 */
public class EncryptedSnapshotTest extends AbstractSnapshotSelfTest {

    private static String SECOND_CACHE_NAME = "encryptedCache";

    /** Parameters. */
    @Parameterized.Parameters(name = "Encryption is always enabled.")
    public static Iterable<Boolean> enableEncryption() {
        return Collections.singletonList(true);
    }

    /** Checks snapshot validati fails if different master key is used. */
    @Test
    public void testCheckSnapshotFailedWithOtherMasterKey() throws Exception {
        IgniteEx ig = startGridsWithCache(1, 1000, key -> new Account(key, key), dfltCacheCfg);

        snp(ig).createSnapshot(SNAPSHOT_NAME).get();

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

    /**
     * Checks re-encryption fails during snapshot restoration.
     */
    @Test
    public void testReencryptDuringRestore() throws Exception {
        testActionDuringSnapshotOperation(true, chageCacheGroupKey(), "Cache group key change was " +
            "rejected.", IgniteException.class);
    }

    /**
     * Checks re-encryption fails during snapshot creation.
     */
    @Test
    public void testReencryptDuringSnapshot() throws Exception {
        testActionDuringSnapshotOperation(false, chageCacheGroupKey(), "Cache group key change was " +
            "rejected.", IgniteException.class);
    }

    /**
     * Checks master key changing failes during snapshot restoration.
     */
    @Test
    public void testMasterKeyChangeDuringRestore() throws Exception {
        testActionDuringSnapshotOperation(true, chageMasterKey(), "Master key change was rejected.",
            IgniteException.class);
    }

    /**
     * Checks master key changing fails during snapshot creation.
     */
    @Test
    public void testMasterKeyChangeDuringSnapshot() throws Exception {
        testActionDuringSnapshotOperation(false, chageMasterKey(), "Master key change was rejected.",
            IgniteException.class);
    }

    /**
     * Checks snapshot-related action is blocked with {@code errPrefix} and {@code errEncrypType} during snapshot restoration or creation.
     *
     * @param restore If {@code true}, snapshot restoration is activated during the test. Snapshot creation otherwise.
     */
    private void testActionDuringSnapshotOperation(boolean restore, Callable<?> action, String errPrefix,
        Class<? extends Exception> errType) throws Exception {
        startGridsWithCache(2, CACHE_KEYS_RANGE, valueBuilder(), defaultCacheConfiguration(),
            defaultCacheConfiguration().setName(SECOND_CACHE_NAME));

        BlockingCustomMessageDiscoverySpi discoSpi = discoSpi(grid(0));

        IgniteFuture<Void> fut;

        if (restore) {
            grid(1).snapshot().createSnapshot(SNAPSHOT_NAME).get(TIMEOUT);

            grid(1).cache(dfltCacheCfg.getName()).destroy();

            awaitPartitionMapExchange();

            discoSpi.block((msg) -> msg instanceof DynamicCacheChangeBatch);

            fut = grid(1).snapshot().restoreSnapshot(SNAPSHOT_NAME, Collections.singleton(DEFAULT_CACHE_NAME));
        }
        else {
            discoSpi.block((msg) -> msg instanceof FullMessage && ((FullMessage)msg).error().isEmpty());

            fut = grid(1).snapshot().createSnapshot(SNAPSHOT_NAME);
        }

        discoSpi.waitBlocked(TIMEOUT);

        GridTestUtils.assertThrowsAnyCause( log, action, errType, errPrefix + " Snapshot operation is in progress.");

        discoSpi.unblock();

        fut.get(TIMEOUT);
    }

    /** Checks snapshot restoration fails if different master key is used. */
    @Test
    public void testRestoreSnapshotFailedWithOtherMasterKey() throws Exception {
        IgniteEx ig = startGridsWithCache(1, 1000, key -> new Account(key, key), dfltCacheCfg);

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

    //TODO
    /** Checks snapshot restoration fails if different master key is contained in the snapshot. */
    @Test
    public void testChangeMe() throws Exception {
        IgniteEx ig = startGridsWithCache(1, 1000, key -> new Account(key, key), dfltCacheCfg);

        snp(ig).createSnapshot(SNAPSHOT_NAME).get();

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

    /** Checks snapshot restoration fails if different master key is contained in the snapshot. */
    @Test
    public void testStartFromSnapshotFailedWithOtherMasterKey() throws Exception {
        IgniteEx ig = startGridsWithCache(1, 1000, key -> new Account(key, key), dfltCacheCfg);

        snp(ig).createSnapshot(SNAPSHOT_NAME).get();

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

    /**
     * @return change cache group key action.
     */
    private Callable<?> chageCacheGroupKey() {
        return ()->grid(1).encryption().changeCacheGroupKey(Collections.singletonList(SECOND_CACHE_NAME)).get(TIMEOUT);
    }

    /**
     * @return change cache group key action.
     */
    private Callable<?> chageMasterKey() {
        return () -> grid(1).encryption().changeMasterKey(AbstractEncryptionTest.MASTER_KEY_NAME_2).get(TIMEOUT);
    }
}
