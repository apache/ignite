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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.IgniteDiagnosticPrepareContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.processors.GridProcessor;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public interface MvccProcessor extends GridProcessor {
    /**
     * Local join callback.
     *
     * @param evt Discovery event.
     * @param discoCache Disco cache.
     */
    void onLocalJoin(DiscoveryEvent evt, DiscoCache discoCache);

    /**
     * Exchange done callback.
     *
     * @param discoCache Disco cache.
     */
    void onExchangeDone(DiscoCache discoCache);

    /**
     * @return Coordinator.
     */
    @NotNull MvccCoordinator currentCoordinator();

    /**
     * @param crdVer Mvcc coordinator version.
     * @param cntr Mvcc counter.
     * @return State for given mvcc version.
     * @throws IgniteCheckedException If fails.
     */
    byte state(long crdVer, long cntr);

    /**
     * @param ver Version to check.
     * @return State for given mvcc version.
     * @throws IgniteCheckedException If fails.
     */
    byte state(MvccVersion ver);

    /**
     * @param ver Version.
     * @param state State.
     * @throws IgniteCheckedException If fails;
     */
    void updateState(MvccVersion ver, byte state);

    /**
     * @param ver Version.
     * @param state State.
     * @param primary Flag if this is primary node.
     * @throws IgniteCheckedException If fails;
     */
    void updateState(MvccVersion ver, byte state, boolean primary);

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
    IgniteInternalFuture<Void> waitFor(GridCacheContext cctx, MvccVersion locked);

    /**
     * @param locked Version the entry is locked by.
     * @throws IgniteCheckedException If failed.
     */
    void releaseWaiters(MvccVersion locked);

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
     */
    MvccSnapshot requestWriteSnapshotLocal();

    /**
     * @return {@link MvccSnapshot} if this is a coordinator node and coordinator is initialized.
     * {@code Null} in other cases.
     */
    MvccSnapshot requestReadSnapshotLocal();

    /**
     * Requests snapshot on Mvcc coordinator.
     *
     * @param crd Expected coordinator.
     * @param lsnr Request listener.
     */
    void requestReadSnapshotAsync(MvccCoordinator crd, MvccSnapshotResponseListener lsnr);

    /**
     * Requests snapshot on Mvcc coordinator.
     *
     * @param crd Expected coordinator.
     * @param lsnr Request listener.
     */
    void requestWriteSnapshotAsync(MvccCoordinator crd, MvccSnapshotResponseListener lsnr);

    /**
     * @param updateVer Transaction update version.
     * @return Acknowledge future.
     */
    IgniteInternalFuture<Void> ackTxCommit(MvccSnapshot updateVer);

    /**
     * @param updateVer Transaction update version.
     */
    void ackTxRollback(MvccVersion updateVer);

    /**
     * @param snapshot Query version.
     * @param qryId Query tracker ID.
     */
    void ackQueryDone(MvccSnapshot snapshot, long qryId);

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
