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

package org.apache.ignite.internal.processors.cache.mvcc;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteDiagnosticPrepareContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.ExchangeContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.util.GridLongList;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
class NoOpMvccProcessor extends GridProcessorAdapter implements MvccProcessor {
    /**
     * @param ctx Kernal context.
     */
    protected NoOpMvccProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void onDiscoveryEvent(int evtType, Collection<ClusterNode> nodes, long topVer) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onExchangeStart(MvccCoordinator mvccCrd, ExchangeContext exchCtx, ClusterNode exchCrd) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onExchangeDone(boolean newCoord, DiscoCache discoCache,
        Map<UUID, GridLongList> activeQueries) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void processClientActiveQueries(UUID nodeId, @Nullable GridLongList activeQueries) {
        throw processorException();
    }

    /** {@inheritDoc} */
    @Nullable @Override public MvccCoordinator currentCoordinator() {
        return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public MvccCoordinator currentCoordinator(AffinityTopologyVersion topVer) {
        return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public MvccCoordinator coordinatorFromDiscoveryEvent() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public UUID currentCoordinatorId() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void updateCoordinator(MvccCoordinator curCrd) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public byte state(long crdVer, long cntr) {
        throw processorException();
    }

    /** {@inheritDoc} */
    @Override public byte state(MvccVersion ver) {
        throw processorException();
    }

    /** {@inheritDoc} */
    @Override public void updateState(MvccVersion ver, byte state) {
        throw processorException();
    }

    /** {@inheritDoc} */
    @Override public void updateState(MvccVersion ver, byte state, boolean primary) {
        throw processorException();
    }

    /** {@inheritDoc} */
    @Override public void registerLocalTransaction(long crd, long cntr) {
        throw processorException();
    }

    /** {@inheritDoc} */
    @Override public boolean hasLocalTransaction(long crd, long cntr) {
        throw processorException();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Void> waitFor(GridCacheContext cctx,
        MvccVersion locked) {
        throw processorException();
    }

    /** {@inheritDoc} */
    @Override public void addQueryTracker(MvccQueryTracker tracker) {
        throw processorException();
    }

    /** {@inheritDoc} */
    @Override public void removeQueryTracker(Long id) {
        throw processorException();
    }

    /** {@inheritDoc} */
    @Override public MvccSnapshot tryRequestSnapshotLocal() {
        throw processorException();
    }

    /** {@inheritDoc} */
    @Override public MvccSnapshot tryRequestSnapshotLocal(
        @Nullable IgniteInternalTx tx) {
        throw processorException();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<MvccSnapshot> requestSnapshotAsync() {
        throw processorException();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<MvccSnapshot> requestSnapshotAsync(IgniteInternalTx tx) {
        throw processorException();
    }

    /** {@inheritDoc} */
    @Override public void requestSnapshotAsync(MvccSnapshotResponseListener lsnr) {
        throw processorException();
    }

    /** {@inheritDoc} */
    @Override public void requestSnapshotAsync(IgniteInternalTx tx, MvccSnapshotResponseListener lsnr) {
        throw processorException();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Void> ackTxCommit(MvccSnapshot updateVer) {
        throw processorException();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Void> ackTxCommit(MvccVersion updateVer, MvccSnapshot readSnapshot,
        long qryId) {
        throw processorException();
    }

    /** {@inheritDoc} */
    @Override public void ackTxRollback(MvccVersion updateVer) {
        throw processorException();
    }

    /** {@inheritDoc} */
   @Override public void ackTxRollback(MvccVersion updateVer, MvccSnapshot readSnapshot, long qryTrackerId) {
       throw processorException();
    }

    /** {@inheritDoc} */
    @Override public void ackQueryDone(MvccSnapshot snapshot, long qryId) {
        throw processorException();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Void> waitTxsFuture(UUID crdId, GridLongList txs) {
        throw processorException();
    }

    /** {@inheritDoc} */
    @Override public void dumpDebugInfo(IgniteLogger log, @Nullable IgniteDiagnosticPrepareContext diagCtx) {
        // No-op.
    }

    /**
     * @return No-op processor usage exception;
     */
    private IgniteException processorException() {
        return new IgniteException("Current Ignite configuration does not support MVCC functionality " +
            "(consider adding ignite-schedule module to classpath).");
    }
}
