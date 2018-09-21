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
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Mvcc tracker.
 */
public interface MvccQueryTracker {
    /** */
    public static final AtomicLong ID_CNTR = new AtomicLong();

    /** */
    public static final long MVCC_TRACKER_ID_NA = -1;

    /**
     * @return Tracker id.
     */
    public long id();

    /**
     * @return Requested MVCC snapshot.
     */
    public MvccSnapshot snapshot();

    /**
     * @return Cache context.
     */
    public GridCacheContext context();

    /**
     * @return Topology version.
     */
    public AffinityTopologyVersion topologyVersion();

    /**
     * Requests version on coordinator.
     *
     * @return Future to wait for result.
     */
    public IgniteInternalFuture<MvccSnapshot> requestSnapshot();

    /**
     * Requests version on coordinator.
     *
     * @param topVer Topology version.
     * @return Future to wait for result.
     */
    public IgniteInternalFuture<MvccSnapshot> requestSnapshot(@NotNull AffinityTopologyVersion topVer);

    /**
     * Requests version on coordinator.
     *
     * @param topVer Topology version.
     * @param lsnr Response listener.
     */
    public void requestSnapshot(@NotNull AffinityTopologyVersion topVer, @NotNull MvccSnapshotResponseListener lsnr);

    /**
     * Marks tracker as done.
     */
    public void onDone();

    /**
     * Marks tracker as done.
     *
     * @param tx Transaction.
     * @param commit Commit flag.
     * @return Acknowledge future.
     */
    @Nullable public IgniteInternalFuture<Void> onDone(@NotNull GridNearTxLocal tx, boolean commit);

    /**
     * Mvcc coordinator change callback.
     *
     * @param newCrd New mvcc coordinator.
     * @return Query id if exists.
     */
    long onMvccCoordinatorChange(MvccCoordinator newCrd);
}
