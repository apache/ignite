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
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.cluster.ClusterTopologyServerNotFoundException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteInClosure;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.noCoordinatorError;

/**
 * Tracker used for an optimistic tx and not-in-tx queries.
 */
@SuppressWarnings("unchecked")
public class MvccQueryTrackerImpl implements MvccQueryTracker {
    /** */
    @GridToStringExclude
    private final GridCacheContext cctx;

    /** */
    @GridToStringExclude
    private final IgniteLogger log;

    /** */
    private long crdVer;

    /** */
    private final long id;

    /** */
    private MvccSnapshot snapshot;

    /** */
    private volatile AffinityTopologyVersion topVer;

    /** */
    private final boolean canRemap;

    /**
     * @param cctx Cache context.
     */
    public MvccQueryTrackerImpl(GridCacheContext cctx) {
        this(cctx, true);
    }

    /**
     * @param cctx Cache context.
     * @param canRemap {@code True} if tracker can remap on coordinator fail.
     */
    public MvccQueryTrackerImpl(GridCacheContext cctx, boolean canRemap) {
        this.cctx = cctx;
        this.id = ID_CNTR.incrementAndGet();
        this.canRemap = canRemap;

        log = cctx.logger(getClass());
    }

    /** {@inheritDoc} */
    @Override public long id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public synchronized MvccSnapshot snapshot() {
        return snapshot;
    }

    /** {@inheritDoc} */
    @Override public GridCacheContext context() {
        return cctx;
    }

    /** {@inheritDoc} */
    @Override public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<MvccSnapshot> requestSnapshot() {
        MvccSnapshot snapshot; MvccSnapshotFuture fut;

        if ((snapshot = snapshot()) != null)
            return new GridFinishedFuture<>(snapshot);

        requestSnapshot0(cctx.shared().exchange().readyAffinityVersion(), fut = new MvccSnapshotFuture());

        return fut;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<MvccSnapshot> requestSnapshot(@NotNull AffinityTopologyVersion topVer) {
        MvccSnapshot snapshot; MvccSnapshotFuture fut;

        if ((snapshot = snapshot()) != null)
            return new GridFinishedFuture<>(snapshot);

        requestSnapshot0(topVer, fut = new MvccSnapshotFuture());

        return fut;
    }

    /** {@inheritDoc} */
    @Override public void requestSnapshot(@NotNull AffinityTopologyVersion topVer, @NotNull MvccSnapshotResponseListener lsnr) {
        MvccSnapshot snapshot = snapshot();

        if (snapshot != null)
            lsnr.onResponse(snapshot);
        else
            requestSnapshot0(topVer, lsnr);
    }

    /** {@inheritDoc} */
    @Override public void onDone() {
        MvccProcessor prc = cctx.shared().coordinators();

        MvccSnapshot snapshot = snapshot();

        if (snapshot != null) {
            prc.removeQueryTracker(id);

            prc.ackQueryDone(snapshot, id);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Void> onDone(@NotNull GridNearTxLocal tx, boolean commit) {
        MvccSnapshot snapshot = snapshot(), txSnapshot = tx.mvccSnapshot();

        if (snapshot == null && txSnapshot == null)
            return commit ? new GridFinishedFuture<>() : null;

        MvccProcessor prc = cctx.shared().coordinators();

        if (snapshot != null)
            prc.removeQueryTracker(id);

        if (txSnapshot == null)
            prc.ackQueryDone(snapshot, id);
        else if (commit)
            return prc.ackTxCommit(txSnapshot, snapshot, id);
        else
            prc.ackTxRollback(txSnapshot, snapshot, id);

        return null;
    }

    /** {@inheritDoc} */
    @Override public synchronized long onMvccCoordinatorChange(MvccCoordinator newCrd) {
        if (snapshot != null) {
            assert crdVer != 0 : this;

            if (crdVer != newCrd.coordinatorVersion()) {
                crdVer = newCrd.coordinatorVersion();

                return id;
            }
            else
                return MVCC_TRACKER_ID_NA;
        }
        else if (crdVer != 0)
            crdVer = 0; // Mark for remap.

        return MVCC_TRACKER_ID_NA;
    }

    /** */
    private void requestSnapshot0(AffinityTopologyVersion topVer, MvccSnapshotResponseListener lsnr) {
        if (checkTopology(topVer, lsnr = decorate(lsnr))) {
            try {
                MvccSnapshot snapshot = cctx.shared().coordinators().tryRequestSnapshotLocal();

                if (snapshot == null)
                    cctx.shared().coordinators().requestSnapshotAsync(lsnr);
                else
                    lsnr.onResponse(snapshot);
            }
            catch (ClusterTopologyCheckedException e) {
                lsnr.onError(e);
            }
        }
    }

    /** */
    private MvccSnapshotResponseListener decorate(MvccSnapshotResponseListener lsnr) {
        assert lsnr != null;

        if (lsnr.getClass() == ListenerDecorator.class)
            return lsnr;

        return new ListenerDecorator(lsnr);
    }

    /**
     * Validates if mvcc snapshot could be requested on the given topology.
     *
     * @return {@code True} if topology is valid.
     */
    private boolean checkTopology(AffinityTopologyVersion topVer, MvccSnapshotResponseListener lsnr) {
        MvccCoordinator crd = cctx.affinity().mvccCoordinator(topVer);

        if (crd == null) {
            lsnr.onError(noCoordinatorError(topVer));

            return false;
        }

        this.topVer = topVer;

        synchronized (this) {
            crdVer = crd.coordinatorVersion();
        }

        MvccCoordinator curCrd = cctx.topology().mvccCoordinator();

        if (!crd.equals(curCrd)) {
            assert cctx.topology().topologyVersionFuture().initialVersion().compareTo(topVer) > 0;

            tryRemap(lsnr);

            return false;
        }

        return true;
    }

    /** */
    private void tryRemap(MvccSnapshotResponseListener lsnr) {
        if (!canRemap) {
            lsnr.onError(new ClusterTopologyCheckedException("Failed to request mvcc version, coordinator failed."));

            return;
        }

        IgniteInternalFuture<AffinityTopologyVersion> waitFut =
            cctx.shared().exchange().affinityReadyFuture(topVer.nextMinorVersion());

        if(log.isDebugEnabled())
            log.debug("Remap on new topology: " + waitFut);

        if (waitFut == null)
            requestSnapshot(cctx.shared().exchange().readyAffinityVersion(), lsnr);
        else {
            waitFut.listen(new IgniteInClosure<IgniteInternalFuture<AffinityTopologyVersion>>() {
                @Override public void apply(IgniteInternalFuture<AffinityTopologyVersion> fut) {
                    try {
                        requestSnapshot(fut.get(), lsnr);
                    }
                    catch (IgniteCheckedException e) {
                        lsnr.onError(e);
                    }
                }
            });
        }
    }

    /**
     * @param res Response.
     * @param lsnr Response listener.
     * @return {@code false} if need to remap.
     */
    private boolean onResponse0(@NotNull MvccSnapshot res, MvccSnapshotResponseListener lsnr) {
        boolean needRemap = false;

        synchronized (this) {
            assert snapshot() == null : "[this=" + this + ", rcvdVer=" + res + "]";

            if (crdVer != 0) {
                this.snapshot = res;
            }
            else
                needRemap = true;
        }

        if (needRemap) { // Coordinator failed or reassigned, need remap.
            tryRemap(lsnr);

            return false;
        }

        cctx.shared().coordinators().addQueryTracker(this);

        return true;
    }

    /**
     * @param e Exception.
     * @param lsnr Response listener.
     * @return {@code false} if need to remap.
     */
    private boolean onError0(IgniteCheckedException e, MvccSnapshotResponseListener lsnr) {
        if (e instanceof ClusterTopologyCheckedException && canRemap) {
            if (e instanceof ClusterTopologyServerNotFoundException)
                return true; // No Mvcc coordinator assigned

            if (log.isDebugEnabled())
                log.debug("Mvcc coordinator failed, need remap: " + e);

            tryRemap(lsnr);

            return false;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MvccQueryTrackerImpl.class, this);
    }

    /** */
    private final class ListenerDecorator implements MvccSnapshotResponseListener {
        /** */
        private final MvccSnapshotResponseListener lsnr;

        /** */
        private ListenerDecorator(MvccSnapshotResponseListener lsnr) {
            this.lsnr = lsnr;
        }

        @Override public void onResponse(MvccSnapshot res) {
            if (onResponse0(res, this))
                lsnr.onResponse(res);
        }

        @Override public void onError(IgniteCheckedException e) {
            if (onError0(e, this))
                lsnr.onError(e);
        }
    }
}
