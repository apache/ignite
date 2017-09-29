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
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteInClosure;
import org.jetbrains.annotations.Nullable;

/**
 * TODO IGNITE-3478: make sure clean up is called when related future is forcibly finished, i.e. on cache stop
 */
public class MvccQueryTracker {
    /** */
    private MvccCoordinator mvccCrd;

    /** */
    private MvccCoordinatorVersion mvccVer;

    /** */
    @GridToStringExclude
    private final GridCacheContext cctx;

    /** */
    private final boolean canRemap;

    /** */
    @GridToStringExclude
    private final MvccQueryAware lsnr;

    /**
     * @param cctx Cache context.
     * @param canRemap {@code True} if can wait for topology changes.
     * @param lsnr Listener.
     */
    public MvccQueryTracker(GridCacheContext cctx, boolean canRemap, MvccQueryAware lsnr) {
        assert cctx.mvccEnabled() : cctx.name();

        this.cctx = cctx;
        this.canRemap = canRemap;
        this.lsnr = lsnr;
    }

    /**
     * @return Requested mvcc version.
     */
    public MvccCoordinatorVersion mvccVersion() {
        assert mvccVer != null : this;

        return mvccVer;
    }

    /** {@inheritDoc} */
    @Nullable public MvccCoordinatorVersion onMvccCoordinatorChange(MvccCoordinator newCrd) {
        synchronized (this) {
            if (mvccVer != null) {
                assert mvccCrd != null : this;

                if (!mvccCrd.equals(newCrd)) {
                    mvccCrd = newCrd; // Need notify new coordinator.

                    return mvccVer;
                }
                else
                    return null;
            }
            else if (mvccCrd != null)
                mvccCrd = null; // Mark for remap.

            return null;
        }
    }

    /**
     *
     */
    public void onQueryDone() {
        MvccCoordinator mvccCrd0 = null;
        MvccCoordinatorVersion mvccVer0 = null;

        synchronized (this) {
            if (mvccVer != null) {
                assert mvccCrd != null;

                mvccCrd0 = mvccCrd;
                mvccVer0 = mvccVer;

                mvccVer = null; // Mark as finished.
            }
        }

        if (mvccVer0 != null)
            cctx.shared().coordinators().ackQueryDone(mvccCrd0, mvccVer0);
    }

    /**
     * @param topVer Topology version.
     */
    public void requestVersion(final AffinityTopologyVersion topVer) {
        MvccCoordinator mvccCrd0 = cctx.affinity().mvccCoordinator(topVer);

        if (mvccCrd0 == null) {
            lsnr.onMvccVersionError(new IgniteCheckedException("Mvcc coordinator is not assigned: " + topVer));

            return;
        }

        synchronized (this) {
            this.mvccCrd = mvccCrd0;
        }

        MvccCoordinator curCrd = cctx.topology().mvccCoordinator();

        if (!mvccCrd0.equals(curCrd)) {
            assert cctx.topology().topologyVersionFuture().initialVersion().compareTo(topVer) > 0;

            if (!canRemap) {
                lsnr.onMvccVersionError(new ClusterTopologyCheckedException("Failed to request mvcc version, coordinator changed."));

                return;
            }
            else {
                waitNextTopology(topVer);

                return;
            }
        }

        IgniteInternalFuture<MvccCoordinatorVersion> cntrFut =
            cctx.shared().coordinators().requestQueryCounter(mvccCrd0);

        cntrFut.listen(new IgniteInClosure<IgniteInternalFuture<MvccCoordinatorVersion>>() {
            @Override public void apply(IgniteInternalFuture<MvccCoordinatorVersion> fut) {
                try {
                    MvccCoordinatorVersion rcvdVer = fut.get();

                    assert rcvdVer != null;

                    boolean needRemap = false;

                    synchronized (MvccQueryTracker.this) {
                        assert mvccVer == null : "[this=" + MvccQueryTracker.this +
                            ", ver=" + mvccVer +
                            ", rcvdVer=" + rcvdVer + "]";

                        if (mvccCrd != null) {
                            mvccVer = rcvdVer;
                        }
                        else
                            needRemap = true;
                    }

                    if (!needRemap) {
                        lsnr.onMvccVersionReceived(topVer);

                        return;
                    }
                }
                catch (ClusterTopologyCheckedException e) {
                    IgniteLogger log = cctx.logger(MvccQueryTracker.class);

                    if (log.isDebugEnabled())
                        log.debug("Mvcc coordinator failed, need remap: " + e);
                }
                catch (IgniteCheckedException e) {
                    lsnr.onMvccVersionError(e);

                    return;
                }

                // Coordinator failed or reassigned, need remap.
                if (canRemap)
                    waitNextTopology(topVer);
                else {
                    lsnr.onMvccVersionError(new ClusterTopologyCheckedException("Failed to " +
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
                        lsnr.onMvccVersionError(e);
                    }
                }
            });
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MvccQueryTracker.class, this);
    }
}
