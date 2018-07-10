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
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.jetbrains.annotations.Nullable;

/**
 * Mvcc tracker.
 */
public interface MvccQueryTracker {

    /**
     * @return Requested MVCC snapshot.
     */
    public MvccSnapshot snapshot();

    /**
     * @return Cache context.
     */
    public GridCacheContext context();

    /**
     * Requests version on coordinator.
     *
     * @param topVer Topology version.
     */
    public void requestVersion(AffinityTopologyVersion topVer);

    /**
     * Marks tracker as done.
     *
     * @return Acknowledge future.
     */
    @Nullable public IgniteInternalFuture<Void> onDone();

    /**
     * Marks tracker as done.
     *
     * @param tx Transaction.
     * @param commit Commit.
     * @return Acknowledge future.
     */
    @Nullable public IgniteInternalFuture<Void> onDone(GridNearTxLocal tx, boolean commit);

    /**
     * Marks tracker as done by error.
     *
     * @param e Exception.
     */
    public void onDone(IgniteCheckedException e);

    /**
     * Mvcc coordinator change callback.
     *
     * @param newCrd New mvcc coordinator.
     * @return Tracker id.
     */
    public long onMvccCoordinatorChange(MvccCoordinator newCrd);

    /**
     * @return Tracker id.
     */
    public long id();
}
