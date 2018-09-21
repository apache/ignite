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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteDiagnosticPrepareContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.GridProcessor;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.ExchangeContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.util.GridLongList;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public interface MvccProcessor extends GridProcessor {
    /**
     * @param evtType Event type.
     * @param nodes Current nodes.
     * @param topVer Topology version.
     * @param customMsg Message
     */
    void onDiscoveryEvent(int evtType, Collection<ClusterNode> nodes, long topVer,
        @Nullable DiscoveryCustomMessage customMsg);

    /**
     * Exchange start callback.
     *
     * @param mvccCrd Mvcc coordinator.
     * @param exchCtx Exchange context.
     * @param exchCrd Exchange coordinator.
     */
    void onExchangeStart(MvccCoordinator mvccCrd, ExchangeContext exchCtx, ClusterNode exchCrd);

    /**
     * Exchange done callback.
     *
     * @param newCoord New coordinator flag.
     * @param discoCache Disco cache.
     * @param activeQueries Active queries.
     */
    void onExchangeDone(boolean newCoord, DiscoCache discoCache, Map<UUID, GridLongList> activeQueries);

    /**
     * @param nodeId Node ID
     * @param activeQueries Active queries.
     */
    void processClientActiveQueries(UUID nodeId, @Nullable GridLongList activeQueries);

    /**
     * @return Mvcc coordinator received from discovery event.
     */
    @Nullable MvccCoordinator assignedCoordinator();

    /**
     * @return Coordinator.
     */
    @Nullable MvccCoordinator currentCoordinator();

    /**
     * Check that the given topology is greater or equals to coordinator's one and returns current coordinator.
     * @param topVer Topology version.
     * @return Mvcc coordinator.
     */
    @Nullable MvccCoordinator currentCoordinator(AffinityTopologyVersion topVer);

    /**
     * @return Current coordinator node ID.
     */
    UUID currentCoordinatorId();

    /**
     * @param curCrd Coordinator.
     */
    void updateCoordinator(MvccCoordinator curCrd);

    /**
     * @param crdVer Mvcc coordinator version.
     * @param cntr Mvcc counter.
     * @return State for given mvcc version.
     * @throws IgniteCheckedException If fails.
     */
    byte state(long crdVer, long cntr) throws IgniteCheckedException;

    /**
     * @param ver Version to check.
     * @return State for given mvcc version.
     * @throws IgniteCheckedException If fails.
     */
    byte state(MvccVersion ver) throws IgniteCheckedException;

    /**
     * @param ver Version.
     * @param state State.
     * @throws IgniteCheckedException If fails;
     */
    void updateState(MvccVersion ver, byte state) throws IgniteCheckedException;

    /**
     * @param ver Version.
     * @param state State.
     * @param primary Flag if this is primary node.
     * @throws IgniteCheckedException If fails;
     */
    void updateState(MvccVersion ver, byte state, boolean primary) throws IgniteCheckedException;

    /**
     * @param crd Mvcc coordinator version.
     * @param cntr Mvcc counter.
     */
    void registerLocalTransaction(long crd, long cntr);

    /**
     * @param crd Mvcc coordinator version.
     * @param cntr Mvcc counter.
     * @return {@code True} if there is an active local transaction with given version.
     */
    boolean hasLocalTransaction(long crd, long cntr);

    /**
     * @param cctx Cache context.
     * @param locked Version the entry is locked by.
     * @return Future, which is completed as soon as the lock is released.
     * @throws IgniteCheckedException If failed.
     */
    IgniteInternalFuture<Void> waitFor(GridCacheContext cctx, MvccVersion locked) throws IgniteCheckedException;

    /**
     * @param tracker Query tracker.
     */
    void addQueryTracker(MvccQueryTracker tracker);

    /**
     * @param id Query tracker id.
     */
    void removeQueryTracker(Long id);

    /**
     * @return {@link MvccSnapshot} if this is a coordinator node and coordinator is initialized.
     * {@code Null} in other cases.
     * @throws ClusterTopologyCheckedException If coordinator doesn't match locked topology or not assigned.
     */
    MvccSnapshot tryRequestSnapshotLocal() throws ClusterTopologyCheckedException;

    /**
     * @param tx Transaction.
     * @return {@link MvccSnapshot} if this is a coordinator node and coordinator is initialized.
     * {@code Null} in other cases.
     * @throws ClusterTopologyCheckedException If coordinator doesn't match locked topology or not assigned.
     */
    MvccSnapshot tryRequestSnapshotLocal(@Nullable IgniteInternalTx tx) throws ClusterTopologyCheckedException;

    /**
     * Requests snapshot on Mvcc coordinator.
     *
     * @return Snapshot future.
     */
    IgniteInternalFuture<MvccSnapshot> requestSnapshotAsync();

    /**
     * Requests snapshot on Mvcc coordinator.
     *
     * @param tx Transaction.
     * @return Snapshot future.
     */
    IgniteInternalFuture<MvccSnapshot> requestSnapshotAsync(IgniteInternalTx tx);

    /**
     * Requests snapshot on Mvcc coordinator.
     *
     * @param lsnr Request listener.
     */
    void requestSnapshotAsync(MvccSnapshotResponseListener lsnr);

    /**
     * Requests snapshot on Mvcc coordinator.
     *
     * @param tx Transaction
     * @param lsnr Request listener.
     */
    void requestSnapshotAsync(IgniteInternalTx tx, MvccSnapshotResponseListener lsnr);

    /**
     * @param updateVer Transaction update version.
     * @return Acknowledge future.
     */
    IgniteInternalFuture<Void> ackTxCommit(MvccSnapshot updateVer);

    /**
     * @param updateVer Transaction update version.
     * @param readSnapshot Transaction read version.
     * @param qryId Query tracker id.
     * @return Acknowledge future.
     */
    IgniteInternalFuture<Void> ackTxCommit(MvccVersion updateVer, MvccSnapshot readSnapshot, long qryId);

    /**
     * @param updateVer Transaction update version.
     */
    void ackTxRollback(MvccVersion updateVer);

    /**
     * @param updateVer Transaction update version.
     * @param readSnapshot Transaction read version.
     * @param qryTrackerId Query tracker id.
     */
    void ackTxRollback(MvccVersion updateVer, MvccSnapshot readSnapshot, long qryTrackerId);

    /**
     * @param snapshot Query version.
     * @param qryId Query tracker ID.
     */
    void ackQueryDone(MvccSnapshot snapshot, long qryId);

    /**
     * @param crdId Coordinator ID.
     * @param txs Transaction IDs.
     * @return Future.
     */
    IgniteInternalFuture<Void> waitTxsFuture(UUID crdId, GridLongList txs);

    /**
     * @param log Logger.
     * @param diagCtx Diagnostic request.
     */
    void dumpDebugInfo(IgniteLogger log, @Nullable IgniteDiagnosticPrepareContext diagCtx);

    /**
     * @return {@code True} if at least one cache with
     * {@code CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT} mode is registered.
     */
    boolean mvccEnabled();

    /**
     * Pre-processes cache configuration before start.
     *
     * @param ccfg Cache configuration to pre-process.
     */
    void preProcessCacheConfiguration(CacheConfiguration ccfg);

    /**
     * Validates cache configuration before start.
     *
     * @param ccfg Cache configuration to validate.
     * @throws IgniteCheckedException If validation failed.
     */
    void validateCacheConfiguration(CacheConfiguration ccfg) throws IgniteCheckedException;

    /**
     * Starts MVCC processor (i.e. initialises data structures and vacuum) if it has not been started yet.
     *
     * @throws IgniteCheckedException If failed to initialize.
     */
    void ensureStarted() throws IgniteCheckedException;
}
