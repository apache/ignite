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
    private enum State {
        /** */
        INIT,

        /** */
        REQUESTING,

        /** */
        CLOSED
    }

    /** */
    @GridToStringExclude
    private final GridCacheContext cctx;

    /** */
    @GridToStringExclude
    private final IgniteLogger log;

    /** */
    private final boolean canRemap;

    /** */
    private final long id;

    /** */
    @GridToStringExclude
    private long crdVer;

    /** */
    private volatile AffinityTopologyVersion topVer;

    /** */
    private Object state = State.INIT;

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
        this.canRemap = canRemap;

        id = ID_CNTR.incrementAndGet();
        log = cctx.logger(getClass());
    }

    /** {@inheritDoc} */
    @Override public long id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public synchronized MvccSnapshot snapshot() {
        assert state != null : this;

        return state.getClass() == State.class ? null : (MvccSnapshot)state;
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
        MvccSnapshot snapshot;
        MvccSnapshotFuture fut;

        if ((snapshot = snapshot()) != null)
            return new GridFinishedFuture<>(snapshot);

        requestSnapshot0(cctx.shared().exchange().readyAffinityVersion(), fut = new MvccSnapshotFuture());

        return fut;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<MvccSnapshot> requestSnapshot(@NotNull AffinityTopologyVersion topVer) {
        MvccSnapshot snapshot;
        MvccSnapshotFuture fut;

        if ((snapshot = snapshot()) != null)
            return new GridFinishedFuture<>(snapshot);

        requestSnapshot0(topVer, fut = new MvccSnapshotFuture());

        return fut;
    }

    /** {@inheritDoc} */
    @Override public void requestSnapshot(@NotNull AffinityTopologyVersion topVer,
        @NotNull MvccSnapshotResponseListener lsnr) {
        MvccSnapshot snapshot = snapshot();

        if (snapshot != null)
            lsnr.onResponse(snapshot);
        else
            requestSnapshot0(topVer, lsnr);
    }

    /** {@inheritDoc} */
    @Override public void onDone() {
        MvccSnapshot snapshot = null;

        synchronized (this) {
            if (state == State.CLOSED)
                return;
            else if (state.getClass() != State.class)
                snapshot = (MvccSnapshot)state;

            state = State.CLOSED;
        }

        cctx.shared().coordinators().removeQueryTracker(id);

        if (snapshot != null)
            cctx.shared().coordinators().ackQueryDone(snapshot, id);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Void> onDone(@NotNull GridNearTxLocal tx, boolean commit) {
        MvccSnapshot snapshot = null;

        synchronized (this) {
            if (state == State.CLOSED) {
                assert !commit : this;

                return null;
            }
            else if (state.getClass() != State.class)
                snapshot = (MvccSnapshot)state;

            state = State.CLOSED;
        }

        cctx.shared().coordinators().removeQueryTracker(id);

        MvccSnapshot txSnapshot = tx.mvccSnapshot();

        if (snapshot == null && txSnapshot == null)
            return commit ? new GridFinishedFuture<>() : null;

        if (txSnapshot == null)
            cctx.shared().coordinators().ackQueryDone(snapshot, id);
        else if (commit)
            return cctx.shared().coordinators().ackTxCommit(txSnapshot, snapshot, id);
        else
            cctx.shared().coordinators().ackTxRollback(txSnapshot, snapshot, id);

        return null;
    }

    /** {@inheritDoc} */
    @Override public synchronized long onMvccCoordinatorChange(MvccCoordinator newCrd) {
        if (snapshot() != null && crdVer != newCrd.coordinatorVersion()) {
            assert crdVer != 0 : this;

            return id;
        }

        if (state == State.REQUESTING)
            crdVer = 0; // Mark for remap.

        return MVCC_TRACKER_ID_NA;
    }

    /**
     * @param topVer Topology version.
     * @param lsnr Response listener.
     */
    private void requestSnapshot0(AffinityTopologyVersion topVer, MvccSnapshotResponseListener lsnr) {
        if (checkActive(lsnr) && checkTopology(topVer, lsnr = decorate(lsnr))) {
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

    /**
     * Decorate response listener if needed.
     *
     * @param lsnr Listener to decorate.
     * @return Decorated listener.
     */
    private MvccSnapshotResponseListener decorate(MvccSnapshotResponseListener lsnr) {
        assert lsnr != null;

        if (lsnr.getClass() == ListenerDecorator.class)
            return lsnr;

        return new ListenerDecorator(lsnr);
    }

    /**
     * Validates if mvcc snapshot could be requested on the given topology.
     *
     * @param topVer Topology version.
     * @param lsnr Response listener.
     * @return {@code True} if topology is valid.
     */
    private boolean checkTopology(AffinityTopologyVersion topVer, MvccSnapshotResponseListener lsnr) {
        MvccCoordinator crd = cctx.affinity().mvccCoordinator(topVer);

        if (crd == null) {
            lsnr.onError(noCoordinatorError(topVer));

            return false;
        }

        boolean closed = false;

        synchronized (this) {
            assert state != null && state.getClass() == State.class : this;

            switch ((State)state) {
                case INIT:
                    cctx.shared().coordinators().addQueryTracker(this);

                    state = State.REQUESTING;

                case REQUESTING:
                    crdVer = crd.coordinatorVersion();

                    break;
                case CLOSED:
                    closed = true;
            }
        }

        if (closed) {
            lsnr.onError(new IgniteCheckedException("Query was closed."));

            return false;
        }

        this.topVer = topVer;

        MvccCoordinator curCrd = cctx.topology().mvccCoordinator();

        if (!crd.equals(curCrd)) {
            assert cctx.topology().topologyVersionFuture().initialVersion().compareTo(topVer) > 0;

            tryRemap(lsnr);

            return false;
        }

        return true;
    }

    /**
     * Validates if mvcc snapshot has been already requested by a client.
     *
     * @return {@code True} if it is a system remap or snapshot has not been requested yet.
     */
    private boolean checkActive(@NotNull MvccSnapshotResponseListener lsnr) {
        if (lsnr.getClass() == ListenerDecorator.class)
            return true;

        IgniteCheckedException ex;

        synchronized (this) {
            if (state == State.INIT)
                return true;
            else if (state == State.CLOSED)
                ex = new IgniteCheckedException("Query was closed.");
            else
                ex = new IgniteCheckedException("Mvcc snapshot has been already requested.");
        }

        lsnr.onError(ex);

        return false;
    }

    /**
     * Tries to remap snapshot request to actual topology.
     *
     * @param lsnr Response listener.
     */
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
     * Response callback.
     *
     * @param res Snapshot result.
     * @param lsnr Response listener.
     * @return {@code false} if need to remap.
     */
    private boolean onResponse0(@NotNull MvccSnapshot res, MvccSnapshotResponseListener lsnr) {
        boolean needRemap = false;

        synchronized (this) {
            assert state == State.REQUESTING || state == State.CLOSED : "[this=" + this + ", rcvdVer=" + res + "]";

            if (state != State.CLOSED) {
                if (crdVer != 0) {
                    this.state = res;

                    return true;
                }
                else
                    needRemap = true;
            }
        }

        if (needRemap) // Coordinator failed or reassigned, need remap.
            tryRemap(lsnr);
        else { // Query was concurrently closed.
            cctx.shared().coordinators().ackQueryDone(res, id);

            lsnr.onError(new IgniteCheckedException("Query was closed."));
        }

        return false;
    }

    /**
     * Error callback.
     *
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

        /** {@inheritDoc} */
        @Override public void onResponse(MvccSnapshot res) {
            if (onResponse0(res, this))
                lsnr.onResponse(res);
        }

        /** {@inheritDoc} */
        @Override public void onError(IgniteCheckedException e) {
            if (onError0(e, this))
                lsnr.onError(e);
        }
    }
}
