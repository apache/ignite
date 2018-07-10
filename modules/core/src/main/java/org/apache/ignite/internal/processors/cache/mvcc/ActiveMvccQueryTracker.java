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

import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteInClosure;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.noCoordinatorError;

/**
 * Tracker used  for an optimistic tx and not-in-tx queries.
 */
@SuppressWarnings("NonPrivateFieldAccessedInSynchronizedContext")
public class ActiveMvccQueryTracker extends TrackableStaticMvccQueryTracker implements MvccSnapshotResponseListener {
    /** */
    private volatile IgniteBiInClosure<AffinityTopologyVersion, IgniteCheckedException> lsnr;

    /** */
    private volatile AffinityTopologyVersion topVer;

    /** */
    private final boolean canRemap;

    /** */
    private UUID crdId;

    /**
     * @param cctx Cache context.
     * @param canRemap {@code True} if can wait for topology changes.
     */
    public ActiveMvccQueryTracker(GridCacheContext cctx, boolean canRemap,
        IgniteBiInClosure<AffinityTopologyVersion, IgniteCheckedException> lsnr) {
        super(cctx, null);
        assert cctx.mvccEnabled() : cctx.name();
        assert lsnr != null;

        this.lsnr = lsnr;
        this.canRemap = canRemap;
    }

    /**
     * @param topVer Topology version.
     */
    @Override public void requestVersion(final AffinityTopologyVersion topVer) {
        assert mvccSnapshot == null;
        assert topVer != null;

        this.topVer = topVer;

        boolean validTop = validateTopologyVersion();

        if (!validTop)
            return;

        MvccSnapshot snapshot = cctx.shared().coordinators().tryRequestSnapshotLocal(null);

        if (snapshot != null)
            onSnapshot(snapshot);
        else
            cctx.shared().coordinators().requestSnapshotAsync(null, this);
    }

    /**
     * Validates if mvcc snapshot could be requested on the given topology.
     *
     * @return {@code True} if topology is valid.
     */
    private boolean validateTopologyVersion() {
        MvccCoordinator mvccCrd0 = cctx.affinity().mvccCoordinator(topVer);

        if (mvccCrd0 == null) {
            onDone(noCoordinatorError(topVer));

            return false;
        }

        synchronized (this) {
            mvccCrdVer = mvccCrd0.coordinatorVersion();
        }

        MvccCoordinator curCrd = cctx.topology().mvccCoordinator();

        if (!mvccCrd0.equals(curCrd)) {
            assert cctx.topology().topologyVersionFuture().initialVersion().compareTo(topVer) > 0;

            if (!canRemap)
                onDone(new ClusterTopologyCheckedException("Failed to request mvcc version, coordinator changed."));
            else
                waitNextTopology();

            return false;
        }

        return true;
    }

    /**
     * @param snapshot Mvcc snapshot.
     */
    private void onSnapshot(MvccSnapshot snapshot) {
        assert snapshot != null;

        boolean needRemap = false;

        synchronized (this) {
            assert mvccSnapshot == null : "[this=" + this +
                ", ver=" + mvccSnapshot +
                ", rcvdVer=" + snapshot + "]";

            // mvccCrd == 0 means it failed during snapshot request and we have to remap to the new topology and coordinator.
            if (mvccCrdVer != 0)
                mvccSnapshot = snapshot;
            else
                needRemap = true;
        }

        if (!needRemap) {
            cctx.shared().coordinators().addQueryTracker(this);

            lsnr.apply(topVer, null);
        }
        else  // Coordinator failed or reassigned, need remap.
            tryRemap();
    }

    /**
     *
     */
    private void tryRemap() {
        if (canRemap)
            waitNextTopology();
        else
            onDone(new ClusterTopologyCheckedException("Failed to request mvcc version, coordinator failed."));
    }

    /**
     *
     */
    @SuppressWarnings("unchecked")
    private void waitNextTopology() {
        assert canRemap;

        IgniteInternalFuture<AffinityTopologyVersion> waitFut =
            cctx.shared().exchange().affinityReadyFuture(topVer.nextMinorVersion());

        if (waitFut == null)
            requestVersion(cctx.shared().exchange().readyAffinityVersion());
        else {
            waitFut.listen(new IgniteInClosure<IgniteInternalFuture<AffinityTopologyVersion>>() {
                @Override public void apply(IgniteInternalFuture<AffinityTopologyVersion> fut) {
                    try {
                        requestVersion(fut.get());
                    }
                    catch (IgniteCheckedException e) {
                        onDone(e);
                    }
                }
            });
        }
    }

    /** {@inheritDoc} */
    @Override public void onResponse(MvccSnapshot res) {
        onSnapshot(res);
    }

    /** {@inheritDoc} */
    @Override public void onError(IgniteCheckedException e) {
        if  (e instanceof ClusterTopologyCheckedException && canRemap) {
            IgniteLogger log = cctx.logger(TrackableStaticMvccQueryTracker.class);

            if (log.isDebugEnabled())
                log.debug("Mvcc coordinator failed, need remap: " + e);

            tryRemap();
        }
        else
            onDone(e);
    }

    /** {@inheritDoc} */
    @Override public void onRequest(UUID nodeId) {
        this.crdId = nodeId;
    }

    /** {@inheritDoc} */
    @Override public UUID coordinatorNodeId() {
        return crdId;
    }

    /** {@inheritDoc} */
    @Override public void onDone(IgniteCheckedException e) {
        super.onDone();

        if (lsnr != null)
            lsnr.apply(null, e);
    }

    /** {@inheritDoc} */
    @Override @Nullable public IgniteInternalFuture<Void> onDone() {
        onDone(null, false);

        return null;
    }

    /** {@inheritDoc} */
    @Override @Nullable public IgniteInternalFuture<Void> onDone(GridNearTxLocal tx, boolean commit) {
        super.onDone();

        MvccSnapshot qrySnapshot = null;

        synchronized (this) {
            if (mvccSnapshot != null) {
                assert mvccCrdVer != 0;

                qrySnapshot = mvccSnapshot;

                mvccSnapshot = null; // Mark as finished.
            }
        }

        if (tx != null && tx.mvccSnapshot() != null)
            return onTxDone(tx.mvccSnapshot(), qrySnapshot, commit);
        else
            onQueryDone(qrySnapshot);

        return null;
    }

    /**
     * @param qrySnapshot Mvcc snapshot.
     */
    private void onQueryDone(MvccSnapshot qrySnapshot) {
        if (qrySnapshot == null)
            return;

        MvccProcessor mvccProcessor = cctx.shared().coordinators();

        if (mvccProcessor.currentCoordinator().coordinatorVersion() == qrySnapshot.coordinatorVersion())
            mvccProcessor.ackQueryDone(qrySnapshot);
    }

    /**
     * @param updateSnapshot Mvcc update info.
     * @param qrySnapshot Query snapshot (for optimistic transactions).
     * @param commit If {@code true} ack commit, otherwise rollback.
     * @return Commit ack future.
     */
    private IgniteInternalFuture<Void> onTxDone(MvccSnapshot updateSnapshot, MvccSnapshot qrySnapshot, boolean commit) {
        assert updateSnapshot != null;

        if (commit)
            return cctx.shared().coordinators().ackTxCommit(updateSnapshot, qrySnapshot, id());
        else
            cctx.shared().coordinators().ackTxRollback(updateSnapshot, qrySnapshot, id());

        return null;
    }
}
