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

import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.CI2;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteInClosure;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.noCoordinatorError;

/**
 *
 */
@SuppressWarnings({"unchecked", "NonPrivateFieldAccessedInSynchronizedContext"})
public class TrackableMvccQueryTracker extends MvccQueryTracker {
    /** */
    public static final long MVCC_TRACKER_ID_NA = -1;

    /** */
    private static final IgniteBiInClosure<AffinityTopologyVersion,IgniteCheckedException> NO_OP_LSNR = new CI2<AffinityTopologyVersion, IgniteCheckedException>() {
        @Override public void apply(AffinityTopologyVersion version, IgniteCheckedException e) {
            // No-op
        }
    };


    /** */
    @GridToStringExclude
    protected final GridCacheContext cctx;

    /** */
    private static final AtomicLong idCntr = new AtomicLong();

    /** */
    private long mvccCrdVer;

    /** */
    private final boolean canRemap;

    /** */
    @GridToStringExclude
    private final IgniteBiInClosure<AffinityTopologyVersion, IgniteCheckedException> lsnr;

    /** */
    private final long id;

    /** */
    private final boolean notifyQryDone;

    /**
     * @param cctx Cache context.
     * @param canRemap {@code True} if can wait for topology changes.
     * @param lsnr Listener.
     */
    public TrackableMvccQueryTracker(GridCacheContext cctx, boolean canRemap,
        @Nullable IgniteBiInClosure<AffinityTopologyVersion, IgniteCheckedException> lsnr) {
        assert cctx.mvccEnabled() : cctx.name();

        this.cctx = cctx;
        this.canRemap = canRemap;
        this.id = idCntr.getAndIncrement();
        this.notifyQryDone = true;

        if (lsnr != null)
            this.lsnr = lsnr;
        else
            this.lsnr = NO_OP_LSNR;
    }

    /**
     * @param cctx Cache context.
     * @param mvccSnapshot Mvcc snapshot.
     */
    public TrackableMvccQueryTracker(GridCacheContext cctx, MvccSnapshot mvccSnapshot) {
        super(mvccSnapshot);
        assert cctx.mvccEnabled() : cctx.name();
        assert cctx != null;
        assert mvccSnapshot != null;


        this.cctx = cctx;
        this.mvccCrdVer = mvccSnapshot.coordinatorVersion();
        this.canRemap = false;
        this.lsnr = NO_OP_LSNR;
        this.id = idCntr.getAndIncrement();
        this.notifyQryDone = false; // It is a tx snapshot. No need to notify coordinator by tracker.

        cctx.shared().coordinators().addQueryTracker(this);
    }


    /**
     * @param newCrd New coordinator.
     * @return Version used by this query.
     */
    public synchronized long onMvccCoordinatorChange(MvccCoordinator newCrd) {
        if (mvccSnapshot != null) {
            assert mvccCrdVer != 0 : this;

            if (mvccCrdVer != newCrd.coordinatorVersion()) {
                mvccCrdVer = newCrd.coordinatorVersion(); // Need notify new coordinator.

                return id;
            }
            else
                return MVCC_TRACKER_ID_NA;
        }
        else if (mvccCrdVer != 0)
            mvccCrdVer = 0; // Mark for remap.

        return MVCC_TRACKER_ID_NA;
    }

    /**
     * @param topVer Topology version.
     */
    public void requestVersion(final AffinityTopologyVersion topVer) {
        boolean validTop = validateTopologyVersion(topVer);

        if (!validTop)
            return;

        MvccSnapshot snapshot = cctx.shared().coordinators().tryRequestSnapshotLocal(false);

        if (snapshot != null)
            onSnapshot(snapshot, topVer);
        else {
            IgniteInternalFuture<MvccSnapshot> snapshotFut = cctx.shared().coordinators().requestSnapshotAsync(false);

            snapshotFut.listen(new IgniteInClosure<IgniteInternalFuture<MvccSnapshot>>() {
                @Override public void apply(IgniteInternalFuture<MvccSnapshot> fut) {
                    try {
                        MvccSnapshot rcvdSnapshot = fut.get();

                        onSnapshot(rcvdSnapshot, topVer);
                    }
                    catch (ClusterTopologyCheckedException e) {
                        IgniteLogger log = cctx.logger(TrackableMvccQueryTracker.class);

                        if (log.isDebugEnabled())
                            log.debug("Mvcc coordinator failed, need remap: " + e);

                        tryRemap(topVer);
                    }
                    catch (IgniteCheckedException e) {
                        onError(e);
                    }
                }
            });
        }
    }

    /** {@inheritDoc} */
    @Override @Nullable public IgniteInternalFuture<Void> onDone() {
        onDone(null, false);

        return null;
    }

    /** {@inheritDoc} */
    @Override @Nullable public IgniteInternalFuture<Void> onDone(GridNearTxLocal tx, boolean commit) {
        MvccSnapshot qrySnapshot = null;

        synchronized (this) {
            if (mvccSnapshot != null) {
                assert mvccCrdVer != 0;

                qrySnapshot = mvccSnapshot;

                mvccSnapshot = null; // Mark as finished.
            }
        }

        cctx.shared().coordinators().removeQueryTracker(id);

        if (tx != null && tx.mvccInfo() != null && tx.mvccInfo().snapshot() != null)
            return onTxDone(tx.mvccInfo().snapshot(), qrySnapshot, commit);
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

        if (mvccProcessor.currentCoordinator().coordinatorVersion() != qrySnapshot.coordinatorVersion())
            mvccProcessor.ackQueryDoneNewCoordinator(id);
        else if (notifyQryDone)
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
            return cctx.shared().coordinators().ackTxCommit(updateSnapshot, qrySnapshot, id);
        else
            cctx.shared().coordinators().ackTxRollback(updateSnapshot, qrySnapshot, id);

        return null;
    }

    /**
     * Validates if mvcc snapshot could be requested on the given topology.
     *
     * @param topVer Topology version.
     * @return {@code True} if topology is valid.
     */
    private boolean validateTopologyVersion(AffinityTopologyVersion topVer) {
        MvccCoordinator mvccCrd0 = cctx.affinity().mvccCoordinator(topVer);

        if (mvccCrd0 == null) {
            onError(noCoordinatorError(topVer));

            return false;
        }

        synchronized (this) {
            mvccCrdVer = mvccCrd0.coordinatorVersion();
        }

        MvccCoordinator curCrd = cctx.topology().mvccCoordinator();

        if (!mvccCrd0.equals(curCrd)) {
            assert cctx.topology().topologyVersionFuture().initialVersion().compareTo(topVer) > 0;

            if (!canRemap) {
                onError(new ClusterTopologyCheckedException("Failed to request mvcc version, coordinator changed."));

                return false;
            }
            else {
                waitNextTopology(topVer);

                return false;
            }
        }

        return true;
    }

    /**
     * @param snapshot Mvcc snapshot.
     * @param topVer Topology version.
     */
    private void onSnapshot(MvccSnapshot snapshot, AffinityTopologyVersion topVer) {
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
            tryRemap(topVer);
    }

    /**
     * @param topVer Topology version.
     */
    private void tryRemap(AffinityTopologyVersion topVer) {
        if (canRemap)
            waitNextTopology(topVer);
        else
            onError(new ClusterTopologyCheckedException("Failed to request mvcc version, coordinator failed."));
    }

    /**
     * @param topVer Current topology version.
     */
    private void waitNextTopology(AffinityTopologyVersion topVer) {
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
                        onError(e);
                    }
                }
            });
        }
    }

    /**
     * @param e Exception.
     */
    private void onError(IgniteCheckedException e) {
        assert e != null;

        cctx.kernalContext().coordinators().removeQueryTracker(id);

        lsnr.apply(null, e);
    }

    /**
     * @return Id.
     */
    public long id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TrackableMvccQueryTracker.class, this);
    }
}
