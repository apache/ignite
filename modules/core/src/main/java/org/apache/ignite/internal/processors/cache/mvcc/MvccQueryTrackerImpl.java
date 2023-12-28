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
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.cluster.ClusterTopologyServerNotFoundException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.NotNull;

/**
 * Tracker used for an optimistic tx and not-in-tx queries.
 */
@SuppressWarnings("unchecked")
public class MvccQueryTrackerImpl implements MvccQueryTracker {
    /** */
    @GridToStringExclude
    private final GridCacheContext cctx;

    /** */
    private long crdVer;

    /** */
    private final long id;

    /** */
    private Object state;

    /** */
    private volatile AffinityTopologyVersion topVer;

    /** */
    private boolean done;

    /**
     * @param cctx Cache context.
     */
    public MvccQueryTrackerImpl(GridCacheContext cctx) {
        this.cctx = cctx;
        this.id = ID_CNTR.incrementAndGet();
    }

    /** {@inheritDoc} */
    @Override public long id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public MvccSnapshot snapshot() {
        Object state0;

        synchronized (this) {
            state0 = state;
        }

        return snapshot(state0);
    }

    /** */
    private MvccSnapshot snapshot(Object state) {
        if (state != null && state.getClass() == SnapshotFuture.class)
            return ((SnapshotFuture)state).result();
        else
            return (MvccSnapshot)state;
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
        SnapshotFuture fut;

        synchronized (this) {
            if (state == null)
                state = fut = new SnapshotFuture();
            else if (state.getClass() == SnapshotFuture.class)
                return (IgniteInternalFuture<MvccSnapshot>)state;
            else
                return new GridFinishedFuture<>((MvccSnapshot)state);
        }

        return fut;
    }

    /** {@inheritDoc} */
    @Override public void onDone() {
        Object state0;

        synchronized (this) {
            if (done)
                return;

            state0 = state;
            done = true;
        }

        if (state0 != null && state0.getClass() == SnapshotFuture.class)
            ((SnapshotFuture)state0).cancel();
    }

    /** {@inheritDoc} */
    @Override public synchronized long onMvccCoordinatorChange(@NotNull MvccCoordinator newCrd) {
        if (snapshot(state) != null) {
            assert crdVer != 0 : this;

            if (crdVer != newCrd.version()) {
                crdVer = newCrd.version();

                return id;
            }
            else
                return MVCC_TRACKER_ID_NA;
        }
        else if (crdVer != 0)
            crdVer = 0; // Mark for remap.

        return MVCC_TRACKER_ID_NA;
    }

    /**
     * @param res Response.
     * @param lsnr Response listener.
     * @return {@code false} if need to remap.
     */
    private boolean onResponse0(@NotNull MvccSnapshot res, @NotNull MvccSnapshotResponseListener lsnr) {
        synchronized (this) {
            assert snapshot(state) == null : "[this=" + this + ", rcvdVer=" + res + "]";

            if (!done && crdVer != 0) {
                this.state = res;

                return true;
            }
        }

        return false;
    }

    /**
     * @param e Exception.
     * @param lsnr Response listener.
     * @return {@code false} if need to remap.
     */
    private boolean onError0(IgniteCheckedException e, @NotNull MvccSnapshotResponseListener lsnr) {
        synchronized (this) {
            if (done)
                return false;
        }

        if (e instanceof ClusterTopologyCheckedException
            && !(e instanceof ClusterTopologyServerNotFoundException))
                return false;

        return true;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MvccQueryTrackerImpl.class, this);
    }

    /** */
    private final class SnapshotFuture extends MvccSnapshotFuture {
        /** */
        private SnapshotFuture() {}

        /** {@inheritDoc} */
        @Override public void onResponse(MvccSnapshot res) {
            if (onResponse0(res, this))
                super.onResponse(res);
        }

        /** {@inheritDoc} */
        @Override public void onError(IgniteCheckedException e) {
            if (onError0(e, this))
                super.onError(e);
        }

        /** {@inheritDoc} */
        @Override public boolean cancel() {
            return onCancelled();
        }
    }
}
