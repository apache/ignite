/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopologyImpl;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Partition to node assignments.
 */
public class GridDhtPreloaderAssignments extends ConcurrentHashMap<ClusterNode, GridDhtPartitionDemandMessage> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final GridDhtPartitionExchangeId exchangeId;

    /** */
    private final AffinityTopologyVersion topVer;

    /** */
    private boolean cancelled;

    /**
     * @param exchangeId Exchange ID.
     * @param topVer Last join order.
     */
    public GridDhtPreloaderAssignments(GridDhtPartitionExchangeId exchangeId, AffinityTopologyVersion topVer) {
        assert exchangeId != null;
        assert topVer.topologyVersion() > 0 : topVer;

        this.exchangeId = exchangeId;
        this.topVer = topVer;
    }

    /**
     * @return {@code True} if assignments creation was cancelled.
     */
    public boolean cancelled() {
        return cancelled;
    }

    /**
     * @param cancelled {@code True} if assignments creation was cancelled.
     */
    public void cancelled(boolean cancelled) {
        this.cancelled = cancelled;
    }

    /**
     * @return Exchange future.
     */
    GridDhtPartitionExchangeId exchangeId() {
        return exchangeId;
    }

    /**
     * @return Topology version based on last {@link GridDhtPartitionTopologyImpl#readyTopVer}.
     */
    public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtPreloaderAssignments.class, this, "exchId", exchangeId,
            "super", super.toString());
    }
}
