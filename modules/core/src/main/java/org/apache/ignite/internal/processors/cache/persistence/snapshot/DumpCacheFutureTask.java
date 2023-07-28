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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.MarshallerContextImpl;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.processors.marshaller.MappedName;
import org.apache.ignite.internal.util.IgniteUtils;

import static org.apache.ignite.internal.processors.cache.GridLocalConfigManager.cachDataFilename;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_DIR_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_GRP_DIR_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.DUMP_LOCK;

/** */
public class DumpCacheFutureTask extends AbstractCreateBackupFutureTask {
    /** */
    private final File dumpDir;

    /**
     * @param cctx Cache context.
     * @param dumpName Dump name.
     * @param srcNodeId Node id which cause snapshot task creation.
     * @param reqId Snapshot operation request ID.
     */
    public DumpCacheFutureTask(
        GridCacheSharedContext<?, ?> cctx,
        UUID srcNodeId,
        UUID reqId,
        String dumpName,
        File dumpDir,
        SnapshotSender snpSndr,
        Map<Integer, Set<Integer>> parts
    ) {
        super(
            cctx,
            srcNodeId,
            reqId,
            dumpName,
            snpSndr,
            parts
        );

        this.dumpDir = dumpDir;
    }

    /** {@inheritDoc} */
    @Override public boolean start() {
        try {
            log.info("Start cache dump [name=" + snpName + ", grps=" + parts.keySet() + ']');

            File dumpNodeDir = IgniteSnapshotManager.nodeDumpDirectory(dumpDir, cctx);

            createDumpLock(dumpNodeDir);

            processPartitions();

            backupAllAsync();
        }
        catch (IgniteCheckedException | IOException e) {
            acceptException(e);

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
    @Override protected List<CompletableFuture<Void>> saveMetaCopy() {
        Collection<BinaryType> types = cctx.kernalContext().cacheObjects().binary().types();

        ArrayList<Map<Integer, MappedName>> mappings = cctx.kernalContext().marshallerContext().getCachedMappings();

        return Arrays.asList(
            CompletableFuture.runAsync(
                wrapExceptionIfStarted(
                    () -> cctx.kernalContext().cacheObjects().saveMetadata(types, dumpDir)
                ),
                snpSndr.executor()
            ),

            CompletableFuture.runAsync(
                wrapExceptionIfStarted(() -> MarshallerContextImpl.saveMappings(cctx.kernalContext(), mappings, dumpDir)),
                snpSndr.executor()
            )
        );
    }

    /** {@inheritDoc} */
    @Override protected List<CompletableFuture<Void>> saveCacheConfigsCopy() {
        try {
            File dumpNodeDir = IgniteSnapshotManager.nodeDumpDirectory(dumpDir, cctx);

            return parts.keySet().stream().map(grp -> CompletableFuture.runAsync(wrapExceptionIfStarted(() -> {
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
            }), snpSndr.executor())).collect(Collectors.toList());
        }
        catch (IgniteCheckedException e) {
            acceptException(e);

            return Collections.emptyList();
        }
    }

    /** {@inheritDoc} */
    @Override protected List<CompletableFuture<Void>> saveGroup(int grp, Set<Integer> grpParts) throws IgniteCheckedException {
        return Collections.singletonList(CompletableFuture.runAsync(wrapExceptionIfStarted(() -> {
            long start = System.currentTimeMillis();

            CacheGroupContext grpCtx = cctx.cache().cacheGroup(grp);

            log.info("Start group dump [name=" + grpCtx.cacheOrGroupName() + ", id=" + grp + ']');

            try {
                Thread.sleep(ThreadLocalRandom.current().nextInt(5_000));
            }
            catch (InterruptedException e) {
                acceptException(e);
            }

            long time = System.currentTimeMillis() - start;

            log.info("Finish group dump [name=" + grpCtx.cacheOrGroupName() + ", id=" + grp + ", time=" + time + ']');
        }), snpSndr.executor()));
    }

    /** {@inheritDoc} */
    @Override protected CompletableFuture<Void> closeAsync() {
        if (closeFut == null) {
            Throwable err0 = err.get();

            Set<GroupPartitionId> taken = new HashSet<>();

            for (Map.Entry<Integer, Set<Integer>> e : processed.entrySet()) {
                int grp = e.getKey();

                for (Integer part : e.getValue())
                    taken.add(new GroupPartitionId(grp, part));
            }

            closeFut = CompletableFuture.runAsync(
                () -> onDone(new SnapshotFutureTaskResult(taken, null), err0),
                cctx.kernalContext().pools().getSystemExecutorService()
            );
        }

        return closeFut;
    }
}
