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
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
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
@SuppressWarnings("unchecked")
public class MvccQueryTracker {
    /** */
    public static final long MVCC_TRACKER_ID_NA = -1;

    /** */
    private static final IgniteBiInClosure<AffinityTopologyVersion,IgniteCheckedException> NO_OP_LSNR = new CI2<AffinityTopologyVersion, IgniteCheckedException>() {
        @Override public void apply(AffinityTopologyVersion version, IgniteCheckedException e) {
            // No-op
        }
    };

    /** */
    private static final AtomicLong idCntr = new AtomicLong();

    /** */
    private MvccCoordinator mvccCrd;

    /** */
    private volatile MvccSnapshot mvccSnapshot;

    /** */
    @GridToStringExclude
    private final GridCacheContext cctx;

    /** */
    private final boolean canRemap;

    /** */
    @GridToStringExclude
    private final IgniteBiInClosure<AffinityTopologyVersion, IgniteCheckedException> lsnr;

    /** */
    private final long id;

    /**
     * @param cctx Cache context.
     * @param canRemap {@code True} if can wait for topology changes.
     * @param lsnr Listener.
     */
    public MvccQueryTracker(GridCacheContext cctx,
        boolean canRemap,
        IgniteBiInClosure<AffinityTopologyVersion, IgniteCheckedException> lsnr)
    {
        assert cctx.mvccEnabled() : cctx.name();

        this.cctx = cctx;
        this.canRemap = canRemap;
        this.lsnr = lsnr;
        this.id = idCntr.getAndIncrement();
    }

    /**
     * @param cctx Cache context.
     * @param mvccCrd Mvcc coordinator.
     * @param mvccSnapshot Mvcc snapshot.
     * @param track Whether this tracker should be tracked by mvcc processor. If {@code True} new mvcc processor will
     * be notified about this query tracker is in progress if query hasn't finished when old coordinator crushed.
     */
    public MvccQueryTracker(GridCacheContext cctx, MvccCoordinator mvccCrd, MvccSnapshot mvccSnapshot, boolean track) {
        assert cctx.mvccEnabled() : cctx.name();

        this.cctx = cctx;
        this.mvccSnapshot = mvccSnapshot;
        this.mvccCrd = mvccCrd;

        canRemap = false;
        lsnr = NO_OP_LSNR;

        if (track) {
            id = idCntr.getAndIncrement();

            cctx.shared().coordinators().addQueryTracker(this);
        }
        else
            id = MVCC_TRACKER_ID_NA;
    }

    /**
     * @return Requested MVCC snapshot.
     */
    public MvccSnapshot snapshot() {
        assert mvccSnapshot != null : this;

        return mvccSnapshot;
    }

    /**
     * @param newCrd New coordinator.
     * @return Version used by this query.
     */
    public synchronized long onMvccCoordinatorChange(MvccCoordinator newCrd) {
        if (mvccSnapshot != null) {
            assert mvccCrd != null : this;

            if (!mvccCrd.equals(newCrd)) {
                mvccCrd = newCrd; // Need notify new coordinator.

                return id;
            }
            else
                return MVCC_TRACKER_ID_NA;
        }
        else if (mvccCrd != null)
            mvccCrd = null; // Mark for remap.

        return MVCC_TRACKER_ID_NA;
    }

    /**
     *
     */
    public void onQueryDone() {
        MvccSnapshot mvccSnapshot0 = null;

        synchronized (this) {
            if (mvccSnapshot != null) {
                assert mvccCrd != null;

                mvccSnapshot0 = mvccSnapshot;

                if (lsnr != NO_OP_LSNR)
                    mvccSnapshot = null; // Mark as finished.
            }
        }

        if (mvccSnapshot0 != null)
            cctx.shared().coordinators().ackQueryDone(lsnr == NO_OP_LSNR, mvccSnapshot0, id);
    }

    /**
     * @param mvccInfo Mvcc update info.
     * @param ctx Context.
     * @param commit If {@code true} ack commit, otherwise rollback.
     * @return Commit ack future.
     */
    public IgniteInternalFuture<Void> onTxDone(@Nullable MvccTxInfo mvccInfo, GridCacheSharedContext ctx, boolean commit) {
        MvccCoordinator mvccCrd0 = null;
        MvccSnapshot mvccSnapshot0 = null;

        synchronized (this) {
            if (mvccSnapshot != null) {
                assert mvccCrd != null;

                mvccCrd0 = mvccCrd;
                mvccSnapshot0 = mvccSnapshot;

                mvccSnapshot = null; // Mark as finished.
            }
        }

        assert mvccSnapshot0 == null || mvccInfo == null || mvccInfo.coordinatorNodeId().equals(mvccCrd0.nodeId());

        if (mvccSnapshot0 != null || mvccInfo != null) {
            if (mvccInfo == null) {
                cctx.shared().coordinators().ackQueryDone(lsnr == NO_OP_LSNR, mvccSnapshot0, id);

                return null;
            }
            else {
                if (commit)
                    return ctx.coordinators().ackTxCommit(mvccInfo.snapshot(), mvccSnapshot0, id);
                else
                    ctx.coordinators().ackTxRollback(mvccInfo.snapshot(), mvccSnapshot0, id);
            }
        }

        return null;
    }

    /**
     * @param topVer Topology version.
     */
    public void requestVersion(final AffinityTopologyVersion topVer) {
        MvccCoordinator mvccCrd0 = cctx.affinity().mvccCoordinator(topVer);

        if (mvccCrd0 == null) {
            onError(noCoordinatorError(topVer));

            return;
        }

        synchronized (this) {
            mvccCrd = mvccCrd0;
        }

        MvccCoordinator curCrd = cctx.topology().mvccCoordinator();

        if (!mvccCrd0.equals(curCrd)) {
            assert cctx.topology().topologyVersionFuture().initialVersion().compareTo(topVer) > 0;

            if (!canRemap) {
                onError(new ClusterTopologyCheckedException("Failed to request mvcc version, coordinator changed."));

                return;
            }
            else {
                waitNextTopology(topVer);

                return;
            }
        }

        IgniteInternalFuture<MvccSnapshot> cntrFut =
            cctx.shared().coordinators().requestQuerySnapshot(mvccCrd0);

        cntrFut.listen(new IgniteInClosure<IgniteInternalFuture<MvccSnapshot>>() {
            @Override public void apply(IgniteInternalFuture<MvccSnapshot> fut) {
                try {
                    MvccSnapshot rcvdSnapshot = fut.get();

                    assert rcvdSnapshot != null;

                    boolean needRemap = false;

                    synchronized (MvccQueryTracker.this) {
                        assert mvccSnapshot == null : "[this=" + MvccQueryTracker.this +
                            ", ver=" + mvccSnapshot +
                            ", rcvdVer=" + rcvdSnapshot + "]";

                        if (mvccCrd != null)
                            mvccSnapshot = rcvdSnapshot;
                        else
                            needRemap = true;
                    }

                    if (!needRemap) {
                        cctx.shared().coordinators().addQueryTracker(MvccQueryTracker.this);

                        lsnr.apply(topVer, null);

                        return;
                    }
                }
                catch (ClusterTopologyCheckedException e) {
                    IgniteLogger log = cctx.logger(MvccQueryTracker.class);

                    if (log.isDebugEnabled())
                        log.debug("Mvcc coordinator failed, need remap: " + e);
                }
                catch (IgniteCheckedException e) {
                    onError(e);

                    return;
                }

                // Coordinator failed or reassigned, need remap.
                if (canRemap)
                    waitNextTopology(topVer);
                else {
                    onError(new ClusterTopologyCheckedException("Failed to " +
                        "request mvcc version, coordinator failed."));
                }
            }
        });
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
        return S.toString(MvccQueryTracker.class, this);
    }
}
