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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.internal.IgniteFutureCancelledCheckedException;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointListener;
import org.apache.ignite.internal.processors.cache.persistence.filename.SnapshotFileTree;
import org.apache.ignite.internal.processors.marshaller.MappedName;
import org.apache.ignite.internal.util.lang.IgniteThrowableRunner;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;

/** */
public abstract class AbstractCreateSnapshotFutureTask extends AbstractSnapshotFutureTask<SnapshotFutureTaskResult> {
    /**
     * Cache group and corresponding partitions collected under the PME lock.
     * For full snapshot additional checkpoint write lock required.
     * @see SnapshotFutureTask#onMarkCheckpointBegin(CheckpointListener.Context)
     */
    protected final Map<Integer, Set<Integer>> processed = new HashMap<>();

    /** Future which will be completed when task requested to be closed. Will be executed on system pool. */
    protected volatile CompletableFuture<Void> closeFut;

    /**
     * @param cctx Shared context.
     * @param srcNodeId Node id which cause snapshot task creation.
     * @param reqId Snapshot operation request ID.
     * @param sft Snapshot file tree.
     * @param snpSndr Factory which produces snapshot sender instance.
     * @param parts Partitions to be processed.
     */
    protected AbstractCreateSnapshotFutureTask(
        GridCacheSharedContext<?, ?> cctx,
        UUID srcNodeId,
        UUID reqId,
        SnapshotFileTree sft,
        SnapshotSender snpSndr,
        Map<Integer, Set<Integer>> parts
    ) {
        super(cctx, srcNodeId, reqId, sft, snpSndr, parts);
    }

    /** */
    protected abstract List<CompletableFuture<Void>> saveCacheConfigs();

    /** */
    protected abstract List<CompletableFuture<Void>> saveGroup(int grpId, Set<Integer> grpParts) throws IgniteCheckedException;

    /** {@inheritDoc} */
    @Override public boolean cancel() {
        super.cancel();

        try {
            closeAsync().get();
        }
        catch (InterruptedException | ExecutionException e) {
            U.error(log, "SnapshotFutureTask cancellation failed", e);

            return false;
        }

        return true;
    }

    /** @return Future which will be completed when operations truly stopped. */
    protected abstract CompletableFuture<Void> closeAsync();

    /**
     * @return {@code true} if current task requested to be stopped.
     */
    protected boolean stopping() {
        return err.get() != null;
    }

    /** */
    protected void processPartitions() throws IgniteCheckedException {
        for (Map.Entry<Integer, Set<Integer>> e : parts.entrySet()) {
            int grpId = e.getKey();
            Set<Integer> grpParts = e.getValue();

            CacheGroupContext gctx = cctx.cache().cacheGroup(grpId);

            Iterator<GridDhtLocalPartition> iter;

            if (grpParts == null)
                iter = gctx.topology().currentLocalPartitions().iterator();
            else {
                if (grpParts.contains(INDEX_PARTITION)) {
                    throw new IgniteCheckedException("Index partition cannot be included into snapshot if " +
                        " set of cache group partitions has been explicitly provided [grpId=" + grpId + ']');
                }

                iter = F.iterator(grpParts, gctx.topology()::localPartition, false);
            }

            Set<Integer> owning = new HashSet<>();
            Set<Integer> missed = new HashSet<>();

            // Iterate over partitions in particular cache group.
            while (iter.hasNext()) {
                GridDhtLocalPartition part = iter.next();

                // Partition can be in MOVING\RENTING states.
                // Index partition will be excluded if not all partition OWNING.
                // There is no data assigned to partition, thus it haven't been created yet.
                if (part.state() == GridDhtPartitionState.OWNING)
                    owning.add(part.id());
                else
                    missed.add(part.id());
            }

            boolean affNode = gctx.nodeFilter() == null || gctx.nodeFilter().apply(cctx.localNode());

            if (grpParts != null) {
                // Partition has been provided for cache group, but some of them are not in OWNING state.
                // Exit with an error.
                if (!missed.isEmpty()) {
                    throw new IgniteCheckedException("Snapshot operation cancelled due to " +
                        "not all of requested partitions has OWNING state on local node [grpId=" + grpId +
                        ", missed=" + S.toStringSortedDistinct(missed) + ']');
                }
            }
            else {
                // Partitions have not been provided for snapshot task and all partitions have
                // OWNING state, so index partition must be included into snapshot.
                if (!missed.isEmpty()) {
                    log.warning("All local cache group partitions in OWNING state have been included into a snapshot. " +
                        "Partitions which have different states skipped. Index partitions has also been skipped " +
                        "[snpName=" + sft.name() + ", grpId=" + grpId + ", missed=" + S.toStringSortedDistinct(missed) + ']');
                }
                else if (affNode && missed.isEmpty() && cctx.kernalContext().query().moduleEnabled())
                    owning.add(INDEX_PARTITION);
            }

            processed.put(grpId, owning);
        }
    }

    /** Starts async execution of all tasks required to create snapshot. */
    protected void saveSnapshotData() {
        try {
            // Submit all tasks for partitions and deltas processing.
            List<CompletableFuture<Void>> futs = new ArrayList<>();

            Collection<BinaryType> binTypesCopy = cctx.kernalContext()
                .cacheObjects()
                .metadata();

            List<Map<Integer, MappedName>> mappingsCopy = cctx.kernalContext()
                .marshallerContext()
                .getCachedMappings();

            // Process binary meta.
            futs.add(runAsync(() -> snpSndr.sendBinaryMeta(binTypesCopy)));
            // Process marshaller meta.
            futs.add(runAsync(() -> snpSndr.sendMarshallerMeta(mappingsCopy)));
            futs.addAll(saveCacheConfigs());

            for (Map.Entry<Integer, Set<Integer>> grpParts : processed.entrySet())
                futs.addAll(saveGroup(grpParts.getKey(), grpParts.getValue()));

            int futsSize = futs.size();

            CompletableFuture.allOf(futs.toArray(new CompletableFuture[futsSize])).whenComplete((res, t) -> {
                assert t == null : "Exception must never be thrown since a wrapper is used " +
                    "for each snapshot task: " + t;

                closeAsync();
            });
        }
        catch (IgniteCheckedException e) {
            acceptException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void acceptException(Throwable th) {
        if (th == null)
            return;

        if (!(th instanceof IgniteFutureCancelledCheckedException))
            U.error(log, "Snapshot task has accepted exception to stop", th);

        if (err.compareAndSet(null, th))
            closeAsync();
    }

    /**
     * @param exec Runnable task to execute.
     * @return Wrapped task.
     */
    Runnable wrapExceptionIfStarted(IgniteThrowableRunner exec) {
        return () -> {
            if (stopping())
                return;

            try {
                exec.run();
            }
            catch (Throwable t) {
                acceptException(t);
            }
        };
    }

    /** */
    protected CompletableFuture<Void> runAsync(IgniteThrowableRunner task) {
        return CompletableFuture.runAsync(
            wrapExceptionIfStarted(task),
            snpSndr.executor()
        );
    }
}
