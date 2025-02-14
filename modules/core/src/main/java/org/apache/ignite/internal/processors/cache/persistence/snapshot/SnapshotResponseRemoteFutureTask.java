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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.cacheDirectory;
import static org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree.partitionFile;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.databaseRelativePath;

/** */
public class SnapshotResponseRemoteFutureTask extends AbstractSnapshotFutureTask<Void> {
    /** Snapshot directory path. */
    private final String snpPath;

    /**
     * @param cctx Shared context.
     * @param srcNodeId Node id which cause snapshot task creation.
     * @param reqId Snapshot operation request ID.
     * @param snpName Unique identifier of snapshot process.
     * @param snpPath Snapshot directory path.
     * @param snpSndr Factory which produces snapshot receiver instance.
     * @param parts Partition to be processed.
     */
    public SnapshotResponseRemoteFutureTask(
        GridCacheSharedContext<?, ?> cctx,
        UUID srcNodeId,
        UUID reqId,
        String snpName,
        String snpPath,
        SnapshotSender snpSndr,
        Map<Integer, Set<Integer>> parts
    ) {
        super(cctx, srcNodeId, reqId, snpName, snpSndr, parts);

        this.snpPath = snpPath;
    }

    /** {@inheritDoc} */
    @Override public boolean start() {
        if (F.isEmpty(parts))
            return false;

        try {
            List<SnapshotMetadata> metas = cctx.snapshotMgr().readSnapshotMetadatas(snpName, snpPath);

            Function<GroupPartitionId, SnapshotMetadata> findMeta = pair -> {
                for (SnapshotMetadata meta : metas) {
                    Map<Integer, Set<Integer>> parts0 = meta.partitions();

                    if (F.isEmpty(parts0))
                        continue;

                    Set<Integer> locParts = parts0.get(pair.getGroupId());

                    if (locParts != null && locParts.contains(pair.getPartitionId()))
                        return meta;
                }

                return null;
            };

            Map<GroupPartitionId, SnapshotMetadata> partsToSend = new HashMap<>();

            parts.forEach((grpId, parts) -> parts.forEach(
                part -> partsToSend.computeIfAbsent(new GroupPartitionId(grpId, part), findMeta)));

            if (partsToSend.containsValue(null)) {
                Collection<GroupPartitionId> missed = F.viewReadOnly(partsToSend.entrySet(), Map.Entry::getKey,
                    e -> e.getValue() == null);

                throw new IgniteException("Snapshot partitions missed on local node " +
                    "[snpName=" + snpName + ", missed=" + missed + ']');
            }

            snpSndr.init(partsToSend.size());

            File snpDir = cctx.snapshotMgr().snapshotLocalDir(snpName, snpPath);

            CompletableFuture.runAsync(() -> partsToSend.forEach((gp, meta) -> {
                if (err.get() != null)
                    return;

                File cacheDir = cacheDirectory(new File(snpDir, databaseRelativePath(meta.folderName())),
                    gp.getGroupId());

                if (cacheDir == null) {
                    throw new IgniteException("Cache directory not found [snpName=" + snpName + ", meta=" + meta +
                        ", pair=" + gp + ']');
                }

                File snpPart = partitionFile(cacheDir.getParentFile(), cacheDir.getName(), gp.getPartitionId());

                if (!snpPart.exists()) {
                    throw new IgniteException("Snapshot partition file not found [cacheDir=" + cacheDir +
                        ", pair=" + gp + ']');
                }

                snpSndr.sendPart(snpPart, cacheDir.getName(), gp, snpPart.length());
            }), snpSndr.executor())
                .whenComplete((r, t) -> {
                    if (t != null)
                        err.compareAndSet(null, t);

                    Throwable th = err.get();

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
