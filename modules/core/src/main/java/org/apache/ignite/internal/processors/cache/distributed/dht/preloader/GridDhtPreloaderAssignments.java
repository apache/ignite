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

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Partition to node assignments.
 */
public class GridDhtPreloaderAssignments extends ConcurrentHashMap<ClusterNode, GridDhtPartitionDemandMessage> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Exchange future. */
    @GridToStringExclude
    private final GridDhtPartitionsExchangeFuture exchFut;

    /** Last join order. */
    private final AffinityTopologyVersion topVer;

    /**
     * @param exchFut Exchange future.
     * @param topVer Last join order.
     */
    public GridDhtPreloaderAssignments(GridDhtPartitionsExchangeFuture exchFut, AffinityTopologyVersion topVer) {
        assert exchFut != null;
        assert topVer.topologyVersion() > 0;

        this.exchFut = exchFut;
        this.topVer = topVer;
    }

    /**
     * @return Exchange future.
     */
    GridDhtPartitionsExchangeFuture exchangeFuture() {
        return exchFut;
    }

    /**
     * @return Topology version.
     */
    AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtPreloaderAssignments.class, this, "exchId", exchFut.exchangeId(),
            "super", super.toString());
    }
}
