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

/**
 * Mvcc tracker.
 */
public interface MvccQueryTracker extends MvccCoordinatorChangeAware {
    /**
     * @return Tracker id.
     */
    default long id() {
        return MVCC_TRACKER_ID_NA;
    }

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
     * Marks tracker as done.
     */
    public void onDone();

}
