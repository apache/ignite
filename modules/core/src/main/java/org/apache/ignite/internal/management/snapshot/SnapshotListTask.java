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

package org.apache.ignite.internal.management.snapshot;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.processors.cache.persistence.filename.SnapshotFileTree;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IncrementalSnapshotMetadata;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotListJobResult;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotListTaskResult;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotMetadata;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.resources.LoggerResource;
import org.jetbrains.annotations.Nullable;

/** */
@GridInternal
public class SnapshotListTask extends VisorMultiNodeTask<SnapshotListCommandArg, SnapshotListTaskResult, SnapshotListJobResult> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<SnapshotListCommandArg, SnapshotListJobResult> job(SnapshotListCommandArg arg) {
        return new SnapshotListJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Override protected Collection<UUID> jobNodes(VisorTaskArgument<SnapshotListCommandArg> arg) {
        /** Allows {@link #map0(List, VisorTaskArgument)} to use the entire subgrid. */
        return ignite.cluster().forServers().nodes().stream().map(ClusterNode::id).collect(Collectors.toList());
    }

    /** {@inheritDoc} */
    @Override protected SnapshotListTaskResult reduce0(List<ComputeJobResult> nodesJobsResults) throws IgniteException {
        String[] cstIds = new String[nodesJobsResults.size()];
        UUID[] nodesIds = new UUID[nodesJobsResults.size()];
        SnapshotListJobResult[] nodesResults = new SnapshotListJobResult[nodesJobsResults.size()];

        for (int i = 0; i < nodesJobsResults.size(); i++) {
            ComputeJobResult nodeJobRes = nodesJobsResults.get(i);

            if (nodeJobRes.getException() != null) {
                throw new IgniteException("Failed to execute snapshot list job on node [uuid=" + nodeJobRes.getNode().id() + ']',
                    nodeJobRes.getException());
            }

            assert nodeJobRes.getData() != null;

            cstIds[i] = nodeConsistentId(nodeJobRes.getNode().id());
            nodesIds[i] = nodeJobRes.getNode().id();
            nodesResults[i] = nodeJobRes.getData();
        }

        return new SnapshotListTaskResult(cstIds, nodesIds, nodesResults);
    }

    /** */
    private String nodeConsistentId(UUID id) {
        ClusterNode n = ignite.context().discovery().node(id);

        if (n == null)
            n = ignite.context().discovery().historicalNode(id);

        return n == null ? "" : n.consistentId().toString();
    }

    /**
     * Walk though a directory. Doesn't lock it or its content. Tries to find files and summarize their size.
     * Tolerates and skips concurrent deletion errors.
     */
    public static long calculateDirectorySize(File path) throws IOException {
        AtomicLong totalSize = new AtomicLong(0);

        Files.walkFileTree(path.toPath(), new SimpleFileVisitor<>() {
            @Override public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                // Use attrs instead of Files.size() for efficiency.
                if (attrs.isRegularFile())
                    totalSize.addAndGet(attrs.size());

                return FileVisitResult.CONTINUE;
            }

            /** File/directory became inaccessible (permission denied, deleted, etc.) */
            @Override public FileVisitResult visitFileFailed(Path file, IOException exc) {
                if (exc instanceof FileNotFoundException)
                    return FileVisitResult.CONTINUE;

                throw new IgniteException("Failed to calculate snapshot size [name=" + path.getName() + ']', exc);
            }
        });

        return totalSize.get();
    }

    /** */
    private static class SnapshotListJob extends SnapshotJob<SnapshotListCommandArg, SnapshotListJobResult> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** */
        @LoggerResource
        private IgniteLogger log;

        /**
         * @param arg Snapshot list task argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected SnapshotListJob(SnapshotListCommandArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected SnapshotListJobResult run(SnapshotListCommandArg arg) {
            assert !ignite.localNode().isClient();

            // Main snapshot data.
            String[] snpNames;
            long[] sizes;
            long[] creationTimes;

            // External storages' info.
            SnapshotListJobResult.SnapshotExtraInfo[] extraStorages = null;

            // Incremental snapshots' info.
            SnapshotListJobResult.SnapshotExtraInfo[] incrementalSnps = null;

            try {
                List<T2<SnapshotFileTree, Long>> locSnps = findLocalSnapshots(
                    arg.src(),
                    ignite.context().cache().context().snapshotMgr().localSnapshotNames(arg.src())
                );

                snpNames = new String[locSnps.size()];
                sizes = new long[locSnps.size()];
                creationTimes = new long[locSnps.size()];

                for (int snpIdx = 0; snpIdx < locSnps.size(); snpIdx++) {
                    SnapshotFileTree sft = locSnps.get(snpIdx).get1();

                    // Main snapshot data.
                    snpNames[snpIdx] = sft.name();
                    creationTimes[snpIdx] = locSnps.get(snpIdx).get2();

                    sizes[snpIdx] = calculateDirectorySize(sft.root());

                    // Optional extra storages data.
                    SnapshotListJobResult.SnapshotExtraInfo snpExtStors = findExtraStorages(sft);

                    if (snpExtStors != null) {
                        if (extraStorages == null)
                            extraStorages = new SnapshotListJobResult.SnapshotExtraInfo[locSnps.size()];

                        extraStorages[snpIdx] = snpExtStors;

                        sizes[snpIdx] += snpExtStors.size();
                    }

                    // Optiona incremental snapshots data.
                    SnapshotListJobResult.SnapshotExtraInfo incRes = incrementalsData(sft);

                    if (incRes != null) {
                        if (incrementalSnps == null)
                            incrementalSnps = new SnapshotListJobResult.SnapshotExtraInfo[locSnps.size()];

                        incrementalSnps[snpIdx] = incRes;
                    }
                }
            }
            catch (Exception e) {
                throw new IgniteException("Failed to list local snapshots [src=" + arg.src() + ']', e);
            }

            return new SnapshotListJobResult(snpNames, sizes, creationTimes, extraStorages, incrementalSnps);
        }

        /** */
        private static @Nullable SnapshotListJobResult.SnapshotExtraInfo findExtraStorages(SnapshotFileTree sft) throws IOException {
            int extStoragesCnt = 0;
            long extStoragesSize = 0;

            for (File es : sft.allStorages().toList()) {
                if (sft.nodeStorage().equals(es))
                    continue;

                extStoragesCnt++;
                extStoragesSize += calculateDirectorySize(es);
            }

            return extStoragesCnt == 0 ? null : new SnapshotListJobResult.SnapshotExtraInfo(extStoragesCnt, extStoragesSize);
        }

        /** @return Snapshot file tree and creation time from the snapshot metadata. */
        private List<T2<SnapshotFileTree, Long>> findLocalSnapshots(@Nullable String customSnpRoot, List<String> folderNames) {
            List<T2<SnapshotFileTree, Long>> res = new ArrayList<>(folderNames.size());

            folderNames.forEach(fn -> {
                // Snapshot tree being used as a path, to read the metas only.
                SnapshotFileTree sft = new SnapshotFileTree(ignite.context(), fn, customSnpRoot);

                List<SnapshotMetadata> metas = ignite.context().cache().context().snapshotMgr().readSnapshotMetadatas(sft, false);

                if (!metas.isEmpty()) {
                    // Get the last-created time snapshot metadata.
                    SnapshotMetadata snpMeta = metas.stream()
                        .max((m0, m1) -> Math.toIntExact(m0.snapshotTime() - m1.snapshotTime()))
                        .orElse(new SnapshotMetadata());

                    // Real, meta-based snapshot file tree. Can belong to other cluster, other baseline.
                    sft = new SnapshotFileTree(
                        ignite.configuration(),
                        ignite.context().pdsFolderResolver().fileTree(),
                        fn,
                        customSnpRoot,
                        snpMeta.folderName(),
                        snpMeta.consistentId()
                    );

                    res.add(new T2<>(sft, snpMeta.snapshotTime()));
                }
            });

            return res;
        }

        /** @return Number, total size and last creation time of incremental snapshots. */
        private @Nullable SnapshotListJobResult.SnapshotExtraInfo incrementalsData(SnapshotFileTree sft) throws IOException {
            File[] incs = sft.incrementsRoot().listFiles();

            int cnt = 0;
            long size = 0L;
            long createTime = 0L;

            if (!F.isEmpty(incs)) {
                IgniteSnapshotManager snpMgr = ignite.context().cache().context().snapshotMgr();

                int incIdx;

                for (File incDir : incs) {
                    try {
                        incIdx = Integer.parseInt(incDir.getName());
                    }
                    catch (NumberFormatException e) {
                        log.warning("Failed to calculate incremental snapshot size, wrong folder name [name="
                            + incDir.getName() + ']', e);

                        continue;
                    }

                    SnapshotFileTree.IncrementalSnapshotFileTree incTree = sft.incrementalSnapshotFileTree(incIdx);
                    IncrementalSnapshotMetadata incMeta;

                    try {
                        incMeta = snpMgr.readIncrementalSnapshotMetadata(incTree.meta());
                    }
                    catch (Exception e) {
                        log.warning("Failed to calculate incremental snapshot size, unable to read the metadata [name="
                            + incDir.getName() + ']', e);

                        continue;
                    }

                    cnt++;
                    size += calculateDirectorySize(incDir);
                    createTime = Math.max(createTime, incMeta.snapshotTime());
                }
            }

            return cnt == 0 ? null : new SnapshotListJobResult.SnapshotExtraInfo(cnt, size, createTime);
        }
    }
}
