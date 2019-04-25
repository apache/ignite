/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.mvcc;

import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.util.future.GridFinishedFuture;

/**
 * Simple MVCC tracker used only as an Mvcc snapshot holder.
 */
public class StaticMvccQueryTracker implements MvccQueryTracker {
    /** */
    private final MvccSnapshot snapshot;
    /** */
    private final GridCacheContext cctx;

    /**
     * @param cctx Cache context.
     * @param snapshot Mvcc snapshot.
     */
    public StaticMvccQueryTracker(GridCacheContext cctx, MvccSnapshot snapshot) {
        this.snapshot = snapshot;
        this.cctx = cctx;
    }

    /** {@inheritDoc} */
    @Override public MvccSnapshot snapshot() {
        assert snapshot != null : this;

        return snapshot;
    }

    /** {@inheritDoc} */
    @Override public GridCacheContext context() {
        return cctx;
    }

    /** {@inheritDoc} */
    @Override public AffinityTopologyVersion topologyVersion() {
        return AffinityTopologyVersion.NONE;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<MvccSnapshot> requestSnapshot() {
        return new GridFinishedFuture<>(snapshot);
    }

    /** {@inheritDoc} */
    @Override public void onDone() {
        // No-op.
    }
}
