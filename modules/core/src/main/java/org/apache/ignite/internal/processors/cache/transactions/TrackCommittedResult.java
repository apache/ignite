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
package org.apache.ignite.internal.processors.cache.transactions;

import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

/**
 * Tuple for result of {@link LocalPendingTransactionsTracker#stopTrackingCommitted()}.
 */
public class TrackCommittedResult {
    /** Transactions committed during tracked period. */
    private final Set<GridCacheVersion> committedTxs;

    /** Graph of dependent (by keys) transactions. */
    private final Map<GridCacheVersion, Set<GridCacheVersion>> dependentTxsGraph;

    /**
     * @param committedTxs Commited txs.
     * @param dependentTxsGraph Dependent txs graph.
     */
    public TrackCommittedResult(
        Set<GridCacheVersion> committedTxs,
        Map<GridCacheVersion, Set<GridCacheVersion>> dependentTxsGraph
    ) {
        this.committedTxs = committedTxs;
        this.dependentTxsGraph = dependentTxsGraph;
    }

    /**
     *
     */
    public Set<GridCacheVersion> committedTxs() {
        return committedTxs;
    }

    /**
     *
     */
    public Map<GridCacheVersion, Set<GridCacheVersion>> dependentTxsGraph() {
        return dependentTxsGraph;
    }
}
