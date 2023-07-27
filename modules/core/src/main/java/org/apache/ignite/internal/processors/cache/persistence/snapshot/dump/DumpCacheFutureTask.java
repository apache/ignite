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

package org.apache.ignite.internal.processors.cache.persistence.snapshot.dump;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.function.BiConsumer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.MarshallerContextImpl;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.AbstractSnapshotFutureTask;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotSender;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.GridLocalConfigManager.cachDataFilename;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_DIR_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_GRP_DIR_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.DUMP_LOCK;

/** */
public class DumpCacheFutureTask extends AbstractSnapshotFutureTask<Void> implements BiConsumer<String, File> {
    /** */
    private final File dumpDir;

    /** */
    private final List<Integer> grps;

    /**
     * @param cctx Cache context.
     * @param dumpName Dump name.
     * @param srcNodeId Node id which cause snapshot task creation.
     * @param reqId Snapshot operation request ID.
     * @param tmpWorkDir Working directory for intermediate snapshot results.
     * @param ioFactory Factory to working with snapshot files.
     */
    public DumpCacheFutureTask(
        GridCacheSharedContext<?, ?> cctx,
        String dumpName,
        UUID srcNodeId,
        UUID reqId,
        @Nullable String snpPath,
        File dumpDir,
        File tmpWorkDir,
        FileIOFactory ioFactory,
        List<Integer> grps
    ) {
        super(
            cctx,
            srcNodeId,
            reqId,
            dumpName,
            tmpWorkDir,
            ioFactory,
            new SnapshotSender(
                cctx.logger(DumpCacheFutureTask.class),
                cctx.kernalContext().pools().getSnapshotExecutorService()
            ) {
                @Override protected void init(int partsCnt) {
                    // No-op.
                }

                @Override protected void sendPart0(File part, String cacheDirName, GroupPartitionId pair, Long length) {
                    // No-op.
                }

                @Override protected void sendDelta0(File delta, String cacheDirName, GroupPartitionId pair) {
                    // No-op.
                }
            },
            null
        );

        this.dumpDir = dumpDir;
        this.grps = grps;

        cctx.cache().configManager().addConfigurationChangeListener(this);
    }

    /** {@inheritDoc} */
    @Override public boolean start() {
        try {
            log.info("Start cache dump [name=" + snpName + ", grps=" + grps + ']');

            File dumpNodeDir = IgniteSnapshotManager.nodeDumpDirectory(dumpDir, cctx);

            createDumpLock(dumpNodeDir);

            for (Integer grp : grps) {
                CacheGroupContext grpCtx = cctx.cache().cacheGroup(grp);

                File grpDir = new File(
                    dumpNodeDir,
                    (grpCtx.caches().size() > 1 ? CACHE_GRP_DIR_PREFIX : CACHE_DIR_PREFIX) + grpCtx.cacheOrGroupName()
                );

                IgniteUtils.ensureDirectory(grpDir, "dump group directory", null);

                for (GridCacheContext<?, ?> cacheCtx : grpCtx.caches()) {
                    CacheConfiguration<?, ?> ccfg = cacheCtx.config();

                    cctx.cache().configManager().writeCacheData(
                        new StoredCacheData(ccfg),
                        new File(grpDir, cachDataFilename(ccfg))
                    );
                }
            }

            cctx.kernalContext().cacheObjects().saveMetadata(
                cctx.kernalContext().cacheObjects().binary().types(),
                dumpDir
            );

            MarshallerContextImpl.saveMappings(cctx.kernalContext(), cctx.kernalContext()
                .marshallerContext()
                .getCachedMappings(), dumpDir);

            onDone();
        }
        catch (IgniteCheckedException | IOException e) {
            onDone(e);
        }

        return false; // Don't wait for checkpoint.
    }

    /** */
    private void createDumpLock(File dumpNodeDir) throws IgniteCheckedException, IOException {
        File lock = new File(dumpNodeDir, DUMP_LOCK);

        if (!lock.createNewFile())
            throw new IgniteCheckedException("Lock file can't be created or already exists: " + lock.getAbsolutePath());
    }

    /** {@inheritDoc} */
    @Override public void acceptException(Throwable th) {

    }

    /** {@inheritDoc} */
    @Override public void accept(String s, File file) {

    }
}
