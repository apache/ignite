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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.processors.cache.persistence.filename.FileTreeUtils;
import org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree;
import org.apache.ignite.internal.processors.cache.persistence.filename.SnapshotFileTree;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

/** */
public class SnapshotResponseRemoteFutureTask extends AbstractSnapshotFutureTask<Void> {
    /** Snapshot file tree. */
    private final SnapshotFileTree sft;

    /**
     * @param cctx Shared context.
     * @param srcNodeId Node id which cause snapshot task creation.
     * @param reqId Snapshot operation request ID.
     * @param sft Snapshot file tree.
     * @param snpSndr Factory which produces snapshot receiver instance.
     * @param parts Partition to be processed.
     */
    public SnapshotResponseRemoteFutureTask(
        GridCacheSharedContext<?, ?> cctx,
        UUID srcNodeId,
        UUID reqId,
        SnapshotFileTree sft,
        SnapshotSender snpSndr,
        Map<Integer, Set<Integer>> parts
    ) {
        super(cctx, srcNodeId, reqId, sft.name(), snpSndr, parts);

        this.sft = sft;
    }

    /** {@inheritDoc} */
    @Override public boolean start() {
        if (F.isEmpty(parts))
            return false;

        try {
            List<SnapshotInfo> metasAndTrees = cctx.snapshotMgr().readSnapshotMetadatas(sft)
                .stream().map(SnapshotInfo::new).collect(Collectors.toList());

            Function<GroupPartitionId, SnapshotInfo> findMeta = pair -> {
                for (SnapshotInfo sinfo : metasAndTrees) {
                    Map<Integer, Set<Integer>> parts0 = sinfo.meta.partitions();

                    if (F.isEmpty(parts0))
                        continue;

                    Set<Integer> locParts = parts0.get(pair.getGroupId());

                    if (locParts != null && locParts.contains(pair.getPartitionId()))
                        return sinfo;
                }

                return null;
            };

            Map<GroupPartitionId, SnapshotInfo> partsToSend = new HashMap<>();

            parts.forEach((grpId, parts) -> parts.forEach(
                part -> partsToSend.computeIfAbsent(new GroupPartitionId(grpId, part), findMeta)));

            if (partsToSend.containsValue(null)) {
                Collection<GroupPartitionId> missed = F.viewReadOnly(partsToSend.entrySet(), Map.Entry::getKey, e -> e.getValue() == null);

                throw new IgniteException("Snapshot partitions missed on local node " +
                    "[snpName=" + snpName + ", missed=" + missed + ']');
            }

            snpSndr.init(partsToSend.size());

            CompletableFuture.runAsync(() -> partsToSend.forEach((gp, sinfo) -> {
                if (err.get() != null)
                    return;

                CacheConfiguration<?, ?> ccfg = F.first(sinfo.groupConfigs(gp.getGroupId())).configuration();

                File snpPart = sinfo.sft.partitionFile(ccfg, gp.getPartitionId());

                if (!snpPart.exists()) {
                    throw new IgniteException("Snapshot partition file not found [" +
                        "cacheDirs=" + Arrays.toString(sinfo.sft.cacheStorages(ccfg)) +
                        ", pair=" + gp + ']');
                }

                snpSndr.sendPart(
                    snpPart,
                    sft.partitionFile(ccfg, gp.getPartitionId()),
                    FileTreeUtils.partitionStorage(ccfg, gp.getPartitionId()),
                    gp,
                    snpPart.length()
                );
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

    /** Snapshot info. */
    private class SnapshotInfo {
        /** Snapshot meta. */
        final SnapshotMetadata meta;

        /** Snapshot file tree. */
        final SnapshotFileTree sft;

        /** Group cache data. */
        final Map<Integer, List<StoredCacheData>> cacheData = new HashMap<>();

        /**
         * @param meta Snapshot meta.
         */
        public SnapshotInfo(SnapshotMetadata meta) {
            this.meta = meta;
            // Separate file tree for the case when snapshot moved from other node.
            this.sft = new SnapshotFileTree(
                cctx.kernalContext(),
                SnapshotResponseRemoteFutureTask.this.sft.name(),
                SnapshotResponseRemoteFutureTask.this.sft.path(),
                meta.folderName(),
                meta.consistentId()
            );
        }

        /**
         * @param grpId Group.
         */
        public List<StoredCacheData> groupConfigs(int grpId) {
            if (cacheData.containsKey(grpId))
                return cacheData.get(grpId);

            List<File> cacheDirs = sft.existingCacheDirectories(grpId);

            if (F.isEmpty(cacheDirs)) {
                throw new IgniteException("Cache directory not found [snpName=" + snpName + ", meta=" + meta +
                    ", grp=" + grpId + ']');
            }

            List<StoredCacheData> res = new ArrayList<>();

            for (File cacheDir : cacheDirs) {
                res.addAll(NodeFileTree.existingCacheConfigFiles(cacheDir).stream().map(f -> {
                    try {
                        return cctx.cache().configManager().readCacheData(f);
                    }
                    catch (IgniteCheckedException e) {
                        throw new IgniteException(e);
                    }
                }).collect(Collectors.toList()));
            }

            if (res.isEmpty()) {
                throw new IgniteException("Cache configs not found [snpName=" + snpName + ", meta=" + meta +
                    ", grp=" + grpId + ']');
            }

            cacheData.put(grpId, res);

            return res;
        }
    }
}
