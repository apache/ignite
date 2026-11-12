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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.processors.cache.persistence.filename.SnapshotFileTree;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IncrementalSnapshotMetadata;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotListJobResult;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotListTaskResult;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotMetadata;
import org.apache.ignite.internal.processors.rollingupgrade.feature.CoreFeatureRegistry;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.thread.pool.IgniteThreadPoolExecutor;
import org.apache.ignite.internal.util.io.GridFileUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.resources.LoggerResource;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.util.lang.ClusterNodeFunc.nodeIds;

/** */
@GridInternal
public class SnapshotListTask extends VisorMultiNodeTask<SnapshotListCommandArg, SnapshotListTaskResult, SnapshotListJobResult> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** */
    public static final String FEATURE_NOT_ACTIVE_ERR_MSG = "Won't search for local snapshots. " +
        "The snapshot list feature isn't activated yet. The Rolling Upgrade has to be finished.";

    /** {@inheritDoc} */
    @Override protected VisorJob<SnapshotListCommandArg, SnapshotListJobResult> job(SnapshotListCommandArg arg) {
        return new SnapshotListJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Override protected Collection<UUID> jobNodes(VisorTaskArgument<SnapshotListCommandArg> arg) {
        if (!ignite.context().rollingUpgrade().features().isActive(CoreFeatureRegistry.SNAPSHOT_LIST_FEATURE))
            throw new IgniteException(FEATURE_NOT_ACTIVE_ERR_MSG);

        /** Allows {@link #map0(List, VisorTaskArgument)} to use the entire grid. */
        return nodeIds(ignite.cluster().forServers().nodes());
    }

    /** {@inheritDoc} */
    @Override protected SnapshotListTaskResult reduce0(List<ComputeJobResult> nodesJobsResults) throws IgniteException {
        String[] consistentIds = new String[nodesJobsResults.size()];
        UUID[] nodesIds = new UUID[nodesJobsResults.size()];
        SnapshotListJobResult[] nodesResults = new SnapshotListJobResult[nodesJobsResults.size()];
        // Errors aren't expected by default.
        IgniteException[] errors = null;

        for (int i = 0; i < nodesJobsResults.size(); i++) {
            ComputeJobResult nodeJobRes = nodesJobsResults.get(i);

            consistentIds[i] = nodeJobRes.getNode().consistentId().toString();
            nodesIds[i] = nodeJobRes.getNode().id();

            if (nodeJobRes.getException() != null) {
                if (errors == null)
                    errors = new IgniteException[nodesJobsResults.size()];

                errors[i] = nodeJobRes.getException();

                continue;
            }

            assert nodeJobRes.getData() != null;

            nodesResults[i] = nodeJobRes.getData();
        }

        return new SnapshotListTaskResult(consistentIds, nodesIds, nodesResults, errors);
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

            if (ignite.context().isStopping())
                throw new IgniteException("Won't search for local snapshots.", new NodeStoppingException("Local node is stopping."));

            // Read local snapshots.
            List<T2<SnapshotFileTree, Long>> locSnps = findLocalSnapshots(arg.src());

            if (locSnps.isEmpty())
                return new SnapshotListJobResult(Collections.emptyMap());

            // Main sizes of the local snapshots (per snapshot name).
            Map<String, Long> sizes = new ConcurrentHashMap<>(locSnps.size(), 1.0f);
            // Optional incremental parts descriptions (per snapshot name).
            Map<String, SnapshotListJobResult.SnapshotInfo> incs = new ConcurrentHashMap<>(locSnps.size(), 1.0f);
            // Optional external storages descriptions (per snapshot name).
            Map<String, SnapshotListJobResult.SnapshotInfo> extStors = new ConcurrentHashMap<>(locSnps.size(), 1.0f);

            // Snapshot sizes, incremental parts and external storages futures.
            List<Future<?>> futs = new ArrayList<>(locSnps.size() * 3);
            // Names of snapshots with read failures.
            Set<String> failedSnps = ConcurrentHashMap.newKeySet(locSnps.size() / 2);

            IgniteThreadPoolExecutor exec = ignite.context().pools().getSnapshotExecutorService();

            // Descriptions of the local snapshots: main sizes, external storages and incremental parts.
            for (T2<SnapshotFileTree, Long> snpPair : locSnps) {
                SnapshotFileTree sft = snpPair.get1();
                String snpName = sft.name();

                // Future for the snapshot main size. It is the largest part of the job, thus it goes first.
                Future<?> fut = exec.submit(() -> {
                    checkNodeIsStopping();

                    if (failedSnps.contains(snpName))
                        return;

                    try {
                        sizes.put(snpName, GridFileUtils.sizeOfDirectory(sft.root()));
                    }
                    catch (Exception e) {
                        failedSnps.add(snpName);

                        log.warning("Failed to calculate snapshot's size, snapshot ignored [snpName='" + snpName + "']", e);
                    }
                });

                futs.add(fut);

                // Future for optional incremental parts.
                fut = exec.submit(() -> {
                    checkNodeIsStopping();

                    if (failedSnps.contains(snpName))
                        return;

                    try {
                        SnapshotListJobResult.SnapshotInfo incDesc = incrementals(sft);

                        if (incDesc != null)
                            incs.put(snpName, incDesc);
                    }
                    catch (Exception e) {
                        failedSnps.add(snpName);

                        log.warning("Failed to read snapshot's incremental parts, snapshot ignored [snpName='" + snpName + "']", e);
                    }
                });

                futs.add(fut);

                // Future for optional external storages.
                fut = exec.submit(() -> {
                    checkNodeIsStopping();

                    if (failedSnps.contains(snpName))
                        return;

                    try {
                        SnapshotListJobResult.SnapshotInfo extDesc = externalStorages(sft);

                        if (extDesc != null)
                            extStors.put(snpName, extDesc);
                    }
                    catch (Exception e) {
                        failedSnps.add(snpName);

                        log.warning("Failed to read snapshot's external storages, snapshot ignored [snpName='" + snpName + "']", e);
                    }
                });

                futs.add(fut);
            }

            // Wait for the futures.
            for (Future<?> fut : futs) {
                try {
                    fut.get();
                }
                catch (ExecutionException e) {
                    throw new IgniteException("Failed to read local node's snapshots.", e);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();

                    throw new IgniteException("Interrupted while reading local snapshots.", e);
                }
            }

            // Result snapshot descriptions.
            Map<String, SnapshotListJobResult.SnapshotInfo> resMap = U.newHashMap(locSnps.size() - failedSnps.size());

            // Reduce results.
            for (T2<SnapshotFileTree, Long> snpPair : locSnps) {
                String snpName = snpPair.get1().name();

                if (failedSnps.contains(snpName))
                    continue;

                Long size = sizes.get(snpName);

                // The size must be calculated unless the snapshot has failed.
                assert size != null;

                SnapshotListJobResult.SnapshotInfo snpDesc = new SnapshotListJobResult.SnapshotInfo(
                    size,
                    snpPair.get2(),
                    extStors.get(snpName),
                    incs.get(snpName)
                );

                resMap.put(snpName, snpDesc);
            }

            return new SnapshotListJobResult(resMap);
        }

        /** */
        private void checkNodeIsStopping() {
            if (ignite.context().isStopping()) {
                throw new IgniteException(
                    "Won't search for local snapshots due to the local node stopping.",
                    new NodeStoppingException("Local node is stopping.")
                );
            }
        }

        /** */
        private @Nullable SnapshotListJobResult.SnapshotInfo externalStorages(SnapshotFileTree sft) {
            int extStoragesCnt = 0;
            long extStoragesSize = 0;

            for (File extStorage : sft.allStorages().toList()) {
                if (sft.nodeStorage().equals(extStorage))
                    continue;

                try {
                    extStoragesSize += GridFileUtils.sizeOfDirectory(extStorage);
                }
                catch (Exception e) {
                    log.warning("Failed to calculate snapshot's external storage size, storage ignored [externalStorage='" +
                        extStorage + "']", e);

                    continue;
                }

                extStoragesCnt++;
            }

            return extStoragesCnt == 0 ? null : new SnapshotListJobResult.SnapshotInfo(extStoragesCnt, extStoragesSize);
        }

        /** @return Snapshot file trees and related snapshots' creation times from the snapshot metadata. */
        private List<T2<SnapshotFileTree, Long>> findLocalSnapshots(@Nullable String snpPath) {
            // The tree is used only to obtain the snapshots root directory. The snapshot name is irrelevant here.
            File[] dirsToParse = new SnapshotFileTree(ignite.context(), "snp", snpPath).root().getParentFile().listFiles();

            if (F.isEmpty(dirsToParse))
                return Collections.emptyList();

            List<Future<T2<SnapshotFileTree, Long>>> futs = new ArrayList<>(dirsToParse.length);

            IgniteThreadPoolExecutor exec = ignite.context().pools().getSnapshotExecutorService();

            for (File snpDir : dirsToParse) {
                Future<T2<SnapshotFileTree, Long>> snpDirFut = exec.submit(() -> {
                    checkNodeIsStopping();

                    String snpName = snpDir.getName();

                    // The tree is used only as a path for reading the snapshot metadata.
                    SnapshotFileTree sft = new SnapshotFileTree(ignite.context(), snpName, snpPath);

                    List<SnapshotMetadata> metas = ignite.context().cache().context().snapshotMgr().readSnapshotMetadatas(sft, false);

                    if (metas.isEmpty())
                        return null;

                    // Get the metadata of the most recently created snapshot.
                    SnapshotMetadata snpMeta = metas.stream().max(Comparator.comparingLong(SnapshotMetadata::snapshotTime)).get();

                    // Snapshot file tree based on its metadata. Holds the real snapshot consistent ID and corresponding
                    // snapshot directory name.
                    sft = new SnapshotFileTree(
                        ignite.configuration(),
                        ignite.context().pdsFolderResolver().fileTree(),
                        snpName,
                        snpPath,
                        snpMeta.folderName(),
                        snpMeta.consistentId()
                    );

                    return new T2<>(sft, snpMeta.snapshotTime());
                });

                futs.add(snpDirFut);
            }

            List<T2<SnapshotFileTree, Long>> res = new ArrayList<>(dirsToParse.length);

            futs.forEach(f -> {
                try {
                    T2<SnapshotFileTree, Long> snpDirRes = f.get();

                    if (snpDirRes != null)
                        res.add(snpDirRes);
                }
                catch (ExecutionException e) {
                    if (X.hasCause(e, NodeStoppingException.class))
                        throw new IgniteException("Won't search for local snapshots.", e.getCause());

                    log.warning("Failed to read snapshot, snapshot ignored.", e);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();

                    throw new IgniteException("Interrupted while reading local snapshots.", e);
                }
            });

            return res;
        }

        /** @return Number, total size and last creation time of incremental snapshots, or {@code null} if there are none. */
        private @Nullable SnapshotListJobResult.SnapshotInfo incrementals(SnapshotFileTree sft) {
            File[] incs = sft.incrementsRoot().listFiles();

            if (F.isEmpty(incs))
                return null;

            int cnt = 0;
            long size = 0L;
            long createTime = 0L;

            IgniteSnapshotManager snpMgr = ignite.context().cache().context().snapshotMgr();

            int incIdx;

            for (File incDir : incs) {
                if (!SnapshotFileTree.incrementSnapshotDir(incDir) || !incDir.exists())
                    continue;

                try {
                    incIdx = Integer.parseInt(incDir.getName());

                    SnapshotFileTree.IncrementalSnapshotFileTree incTree = sft.incrementalSnapshotFileTree(incIdx);

                    IncrementalSnapshotMetadata incMeta = snpMgr.readIncrementalSnapshotMetadata(incTree.meta());

                    size += GridFileUtils.sizeOfDirectory(incDir);

                    createTime = Math.max(createTime, incMeta.snapshotTime());

                    cnt++;
                }
                catch (Exception e) {
                    log.warning("Failed to read incremental snapshot, incremental part ignored [dir='" + incDir + "']", e);
                }
            }

            return cnt == 0 ? null : new SnapshotListJobResult.SnapshotInfo(cnt, size, createTime);
        }
    }
}
