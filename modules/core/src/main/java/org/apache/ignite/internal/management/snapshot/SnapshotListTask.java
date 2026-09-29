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
import java.nio.file.attribute.FileTime;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotListJobResult;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotListTaskResult;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.internal.visor.VisorTaskArgument;
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
    public static T2<Long, FileTime> calculateDirectorySize(File path) throws IOException {
        AtomicLong totalSize = new AtomicLong(0);

        AtomicReference<FileTime> createTime = new AtomicReference<>();

        Files.walkFileTree(path.toPath(), new SimpleFileVisitor<>() {
            @Override public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                createTime.compareAndSet(null, attrs.creationTime());

                return super.preVisitDirectory(dir, attrs);
            }

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

        return new T2<>(totalSize.get(), createTime.get());
    }

    /** */
    private static class SnapshotListJob extends SnapshotJob<SnapshotListCommandArg, SnapshotListJobResult> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

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

            IgniteSnapshotManager snpMgr = ignite.context().cache().context().snapshotMgr();

            String[] snpNames;
            long[] sizes;
            long[] creationTimes;

            try {
                File resolvedPath = resolveSnapshotsPath(arg.src());

                List<String> locSnps = snpMgr.localSnapshotNames(resolvedPath.getAbsolutePath());

                snpNames = new String[locSnps.size()];
                sizes = new long[locSnps.size()];
                creationTimes = new long[locSnps.size()];

                for (int s = 0; s < locSnps.size(); s++) {
                    String snpName = locSnps.get(s);

                    snpNames[s] = snpName;

                    File snpPath = new File(resolvedPath, snpName);

                    T2<Long, FileTime> snpRes = calculateDirectorySize(snpPath);

                    sizes[s] = snpRes.get1();
                    // TODO: check the time conversion.
                    creationTimes[s] = snpRes.get2().toInstant().getEpochSecond();
                }
            }
            catch (Exception e) {
                throw new IgniteException("Failed to list local snapshots [src=" + arg.src() + ']', e);
            }

            return new SnapshotListJobResult(snpNames, sizes, creationTimes);
        }

        /** TODO: move to a shared with the deletion process place. */
        private File resolveSnapshotsPath(@Nullable String src) throws IOException {
            File res = ignite.context().pdsFolderResolver().fileTree().snapshotsRoot();

            if (src != null) {
                File srcF = new File(src);

                if (srcF.isAbsolute())
                    res = srcF;
                else
                    res = new File(res, src);
            }

            return res.getCanonicalFile();
        }
    }
}
