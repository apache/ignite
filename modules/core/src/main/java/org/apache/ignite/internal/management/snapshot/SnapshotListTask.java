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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.processors.cache.persistence.filename.SnapshotFileTree;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IncrementalSnapshotMetadata;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotListJobResult;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotListTaskResult;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotMetadata;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.thread.pool.IgniteThreadPoolExecutor;
import org.apache.ignite.internal.util.typedef.F;
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
        /** erial version uid. */
        private static final long serialVersionUID = 0L;

        /** */
        @LoggerResource
        private IgniteLogger log;

        /**
         * @param arg   Snapshot list task argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected SnapshotListJob(SnapshotListCommandArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected SnapshotListJobResult run(SnapshotListCommandArg arg) {
            assert !ignite.localNode().isClient();

            if (ignite.context().isStopping())
                throw new IgniteException("Won't search for local snapshots.", new NodeStoppingException("Node is stopping."));

            return new SnapshotListJobResult(findLocalSnapshots(arg.src()));
        }

        /** */
        private static @Nullable SnapshotListJobResult.SnapshotInfo externalStorages(
            SnapshotFileTree sft) throws IOException {
            int extStoragesCnt = 0;
            long extStoragesSize = 0;

            for (File es : sft.allStorages().toList()) {
                if (sft.nodeStorage().equals(es))
                    continue;

                extStoragesCnt++;
                extStoragesSize += calculateDirectorySize(es);
            }

            return extStoragesCnt == 0 ? null : new SnapshotListJobResult.SnapshotInfo(extStoragesCnt, extStoragesSize);
        }

        /** @return Snapshot file tree and creation time from the snapshot metadata. */
        private Map<String, SnapshotListJobResult.SnapshotInfo> findLocalSnapshots(@Nullable String snpPath) {
            // The tree is used only to extract the snapshots root directory. The snapshot name isn't used.
            File[] dirsToParse = new SnapshotFileTree(ignite.context(), "snp", snpPath).root().getParentFile().listFiles();

            if (F.isEmpty(dirsToParse))
                return Collections.emptyMap();

            // Per snapshot/directory futures.
            List<Future<?>> futs = new ArrayList<>(dirsToParse.length);

            Map<String, SnapshotListJobResult.SnapshotInfo> res = new ConcurrentHashMap<>(dirsToParse.length, 1.0f);

            for (File snpDir : dirsToParse) {
                // Single snapshot/directory future.
                Future<?> snpDirFut = ignite.context().pools().getSnapshotExecutorService().submit(() -> {
                    try {
                        res.put(snpDir.getName(), readSingleSnapshot(snpDir, snpPath));
                    }
                    catch (Exception e) {
                        throw new IgniteException("Failed to read snapshot [dir=" + snpDir + ']', e);
                    }
                });

                futs.add(snpDirFut);
            }

            futs.forEach(f -> {
                try {
                    f.get();
                }
                catch (Exception e) {
                    log.warning("Failed to calculate snapshot size, ignoring snapshot [path=" + snpPath + ']', e);
                }
            });

            return res;
        }

        /** */
        private @Nullable SnapshotListJobResult.SnapshotInfo readSingleSnapshot(File snpDir, @Nullable String snpPath) throws Exception {
            String snpName = snpDir.getName();

            // Snapshot tree being used as a path, to read the metas only.
            SnapshotFileTree sft = new SnapshotFileTree(ignite.context(), snpName, snpPath);

            List<SnapshotMetadata> metas = ignite.context().cache().context().snapshotMgr().readSnapshotMetadatas(sft, false);

            if (metas.isEmpty())
                return null;

            // Get the last-created time snapshot metadata.
            SnapshotMetadata snpMeta = metas.stream()
                .max((m0, m1) -> Math.toIntExact(m0.snapshotTime() - m1.snapshotTime()))
                .orElse(new SnapshotMetadata());

            // Real, meta-based snapshot file tree. Can belong to other cluster, other consistent id.
            sft = new SnapshotFileTree(
                ignite.configuration(),
                ignite.context().pdsFolderResolver().fileTree(),
                snpName,
                snpPath,
                snpMeta.folderName(),
                snpMeta.consistentId()
            );

            SnapshotListJobResult.SnapshotInfo incs = readIncrementals(sft);

            SnapshotListJobResult.SnapshotInfo extStors = externalStorages(sft);

            return new SnapshotListJobResult.SnapshotInfo(
                calculateDirectorySize(sft.root()),
                snpMeta.snapshotTime(),
                extStors,
                incs
            );
        }

        /** */
        private @Nullable SnapshotListJobResult.SnapshotInfo readIncrementals(SnapshotFileTree sft) throws Exception {
            // Optional incremental parts.
            File[] incsFiles = sft.incrementsRoot().listFiles();

            if (F.isEmpty(incsFiles))
                return null;

            IgniteThreadPoolExecutor exeSrvc = ignite.context().pools().getSnapshotExecutorService();

            AtomicInteger cnt = new AtomicInteger();
            AtomicLong size = new AtomicLong();
            AtomicLong createTime = new AtomicLong();

            List<Future<?>> futs = Collections.emptyList();

            for (File incDir : incsFiles) {
                if (exeSrvc.getMaximumPoolSize() == 1)
                    incrementalPartWorkUnit(sft, incDir, cnt, size, createTime);
                else {
                    Future<?> fut = exeSrvc.submit(() -> {
                        try {
                            incrementalPartWorkUnit(sft, incDir, cnt, size, createTime);
                        }
                        catch (Exception e) {
                            throw new IgniteException("Failed to calculate incremental snapshot size [dir=" + incDir + ']', e);
                        }
                    });

                    if (futs == Collections.EMPTY_LIST)
                        futs = new ArrayList<>(incsFiles.length);

                    futs.add(fut);
                }
            }

            for (Future<?> fut : futs)
                fut.get();

            return new SnapshotListJobResult.SnapshotInfo(cnt.get(), size.get(), createTime.get());
        }

        /** */
        private void incrementalPartWorkUnit(
            SnapshotFileTree sft,
            File incDir,
            AtomicInteger cnt,
            AtomicLong size,
            AtomicLong createTime
        ) throws Exception {
            int incIdx = Integer.parseInt(incDir.getName());

            SnapshotFileTree.IncrementalSnapshotFileTree incTree = sft.incrementalSnapshotFileTree(incIdx);

            IncrementalSnapshotMetadata incMeta = ignite.context().cache().context().snapshotMgr()
                .readIncrementalSnapshotMetadata(incTree.meta());

            cnt.incrementAndGet();
            size.addAndGet(calculateDirectorySize(incDir));

            createTime.accumulateAndGet(incMeta.snapshotTime(), (l, r)->Math.max(l, r));
        }
    }
}
