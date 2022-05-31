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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

import static java.util.Optional.ofNullable;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.cacheDirectory;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.getPartitionFile;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.databaseRelativePath;

/** */
public class SnapshotResponseRemoteFutureTask extends AbstractSnapshotFutureTask<Void> {
    /** Snapshot directory path. */
    private final String snpPath;

    /**
     * @param cctx Shared context.
     * @param srcNodeId Node id which cause snapshot task creation.
     * @param snpName Unique identifier of snapshot process.
     * @param snpPath Snapshot directory path.
     * @param tmpWorkDir Working directory for intermediate snapshot results.
     * @param ioFactory Factory to working with snapshot files.
     * @param snpSndr Factory which produces snapshot receiver instance.
     */
    public SnapshotResponseRemoteFutureTask(
        GridCacheSharedContext<?, ?> cctx,
        UUID srcNodeId,
        String snpName,
        String snpPath,
        File tmpWorkDir,
        FileIOFactory ioFactory,
        SnapshotSender snpSndr,
        Map<Integer, Set<Integer>> parts
    ) {
        super(cctx, srcNodeId, snpName, tmpWorkDir, ioFactory, snpSndr, parts);

        this.snpPath = snpPath;
    }

    /** {@inheritDoc} */
    @Override public boolean start() {
        if (F.isEmpty(parts))
            return false;

        try {
            List<GroupPartitionId> handled = new ArrayList<>();

            for (Map.Entry<Integer, Set<Integer>> e : parts.entrySet()) {
                ofNullable(e.getValue()).orElse(Collections.emptySet())
                    .forEach(p -> handled.add(new GroupPartitionId(e.getKey(), p)));
            }

            snpSndr.init(handled.size());

            File snpDir = cctx.snapshotMgr().snapshotLocalDir(snpName, snpPath);

            List<CompletableFuture<Void>> futs = new ArrayList<>();
            List<SnapshotMetadata> metas = cctx.snapshotMgr().readSnapshotMetadatas(snpName, snpPath);

            for (SnapshotMetadata meta : metas) {
                Map<Integer, Set<Integer>> parts0 = meta.partitions();

                if (F.isEmpty(parts0))
                    continue;

                handled.removeIf(gp -> {
                    if (ofNullable(parts0.get(gp.getGroupId()))
                        .orElse(Collections.emptySet())
                        .contains(gp.getPartitionId())
                    ) {
                        futs.add(CompletableFuture.runAsync(() -> {
                            if (err.get() != null)
                                return;

                            File cacheDir = cacheDirectory(new File(snpDir, databaseRelativePath(meta.folderName())),
                                gp.getGroupId());

                            if (cacheDir == null) {
                                throw new IgniteException("Cache directory not found [snpName=" + snpName + ", meta=" + meta +
                                    ", pair=" + gp + ']');
                            }

                            File snpPart = getPartitionFile(cacheDir.getParentFile(), cacheDir.getName(), gp.getPartitionId());

                            if (!snpPart.exists()) {
                                throw new IgniteException("Snapshot partition file not found [cacheDir=" + cacheDir +
                                    ", pair=" + gp + ']');
                            }

                            snpSndr.sendPart(snpPart, cacheDir.getName(), gp, snpPart.length());
                        }, snpSndr.executor())
                            .whenComplete((r, t) -> err.compareAndSet(null, t)));

                        return true;
                    }

                    return false;
                });
            }

            if (!handled.isEmpty()) {
                err.compareAndSet(null, new IgniteException("Snapshot partitions missed on local node [snpName=" + snpName +
                    ", missed=" + handled + ']'));
            }

            int size = futs.size();

            CompletableFuture.allOf(futs.toArray(new CompletableFuture[size]))
                .whenComplete((r, t) -> {
                    Throwable th = ofNullable(err.get()).orElse(t);

                    if (th == null && log.isInfoEnabled()) {
                        log.info("Snapshot partitions have been sent to the remote node [snpName=" + snpName +
                            ", rmtNodeId=" + srcNodeId + ']');
                    }

                    close(th);
                });

            return true;
        }
        catch (Throwable t) {
            if (err.compareAndSet(null, t))
                close(t);

            return false;
        }
    }

    /** {@inheritDoc} */
    @Override public void acceptException(Throwable th) {
        if (err.compareAndSet(null, th))
            close(th);
    }

    /**
     * @param th Additional close exception if occurred.
     */
    private void close(@Nullable Throwable th) {
        if (th == null) {
            snpSndr.close(null);
            onDone((Void)null);
        }
        else {
            snpSndr.close(th);
            onDone(th);
        }
    }
}
