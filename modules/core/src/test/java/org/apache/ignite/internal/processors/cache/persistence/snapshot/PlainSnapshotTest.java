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
import java.util.Collections;
import java.util.Map;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.filename.PdsFolderSettings;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.junit.Test;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.MarshallerContextImpl.mappingFileStoreWorkDir;
import static org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl.binaryWorkDir;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.cacheDirName;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.databaseRelativePath;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;

/**
 * Snapshot test for plain, not-encrypted-only snapshots.
 */
public class PlainSnapshotTest extends AbstractSnapshotSelfTest {
    /** Parameters. */
    @Parameterized.Parameters(name = "Encryption is disabled.")
    public static Iterable<Boolean> disableEncryption() {
        return Collections.singletonList(false);
    }

    /** {@link AbstractSnapshotSelfTest.Account} with custom toString method. */
    private static class AccountOverrideToString extends AbstractSnapshotSelfTest.Account {
        /**
         * @param id      User id.
         * @param balance User balance.
         */
        public AccountOverrideToString(int id, int balance) {
            super(id, balance);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "_" + super.toString();
        }
    }

    /**
     * Checks, compares CRCs of partitions in snapshot data files against the source.
     * <p>
     * <b>NOTE:</b>
     * This test is actual only for not-encrypted snapshots.
     * Writing the deltas causes several page writes into file. Every page write calls encrypt(). Every repeatable
     * encrypt() produces different record. Even for same original data. Re-writing pages from delta to partition file
     * in the snapshot issues additional encryption before writing to the snapshot partition file. Thus, page in
     * original partition and in snapshot partition has different encrypted CRC and same decrypted CRC. Different
     * encrypted CRC looks like different data in point of view of third-party observer.
     *
     * @throws Exception If fails.
     */
    @Test
    public void testSnapshotLocalPartitions() throws Exception {
        IgniteEx ig = startGridsWithCache(1, 4096, key -> new AbstractSnapshotSelfTest.Account(key, key),
            new CacheConfiguration<>(DEFAULT_CACHE_NAME));

        for (int i = 4096; i < 8192; i++)
            ig.cache(DEFAULT_CACHE_NAME).put(i, new AccountOverrideToString(i, i));

        GridCacheSharedContext<?, ?> cctx = ig.context().cache().context();
        IgniteSnapshotManager mgr = snp(ig);

        // Collection of pairs group and appropriate cache partition to be snapshot.
        IgniteInternalFuture<?> snpFut = startLocalSnapshotTask(cctx,
            SNAPSHOT_NAME,
            F.asMap(CU.cacheId(DEFAULT_CACHE_NAME), null),
            false, mgr.localSnapshotSenderFactory().apply(SNAPSHOT_NAME, null));

        snpFut.get();

        File cacheWorkDir = ((FilePageStoreManager)ig.context()
            .cache()
            .context()
            .pageStore())
            .cacheWorkDir(dfltCacheCfg);

        // Checkpoint forces on cluster deactivation (currently only single node in cluster),
        // so we must have the same data in snapshot partitions and those which left
        // after node stop.
        stopGrid(ig.name());

        // Calculate CRCs.
        IgniteConfiguration cfg = ig.context().config();
        PdsFolderSettings settings = ig.context().pdsFolderResolver().resolveFolders();
        String nodePath = databaseRelativePath(settings.folderName());
        File binWorkDir = binaryWorkDir(cfg.getWorkDirectory(), settings.folderName());
        File marshWorkDir = mappingFileStoreWorkDir(U.workDirectory(cfg.getWorkDirectory(), cfg.getIgniteHome()));
        File snpBinWorkDir = binaryWorkDir(mgr.snapshotLocalDir(SNAPSHOT_NAME).getAbsolutePath(), settings.folderName());
        File snpMarshWorkDir = mappingFileStoreWorkDir(mgr.snapshotLocalDir(SNAPSHOT_NAME).getAbsolutePath());

        final Map<String, Integer> origPartCRCs = calculateCRC32Partitions(cacheWorkDir);
        final Map<String, Integer> snpPartCRCs = calculateCRC32Partitions(
            FilePageStoreManager.cacheWorkDir(U.resolveWorkDirectory(mgr.snapshotLocalDir(SNAPSHOT_NAME)
                    .getAbsolutePath(),
                nodePath,
                false),
                cacheDirName(dfltCacheCfg)));

        assertEquals("Partitions must have the same CRC after file copying and merging partition delta files",
            origPartCRCs, snpPartCRCs);
        assertEquals("Binary object mappings must be the same for local node and created snapshot",
            calculateCRC32Partitions(binWorkDir), calculateCRC32Partitions(snpBinWorkDir));
        assertEquals("Marshaller meta mast be the same for local node and created snapshot",
            calculateCRC32Partitions(marshWorkDir), calculateCRC32Partitions(snpMarshWorkDir));

        File snpWorkDir = mgr.snapshotTmpDir();

        assertEquals("Snapshot working directory must be cleaned after usage", 0, snpWorkDir.listFiles().length);
    }

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotInMemoryFail() throws Exception {
        persistence = false;

        IgniteEx srv = startGrid(0);

        srv.cluster().state(ACTIVE);

        IgniteEx clnt = startClientGrid(1);

        IgniteFuture<?> fut = clnt.snapshot().createSnapshot(SNAPSHOT_NAME);

        assertThrowsAnyCause(log,
            fut::get,
            IgniteException.class,
            "Snapshots on an in-memory clusters are not allowed.");
    }
}
