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

package org.apache.ignite.cdc;

import java.io.Serializable;
import java.util.Objects;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteExperimental;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.version.GridCacheVersion.DR_ID_MASK;
import static org.apache.ignite.internal.processors.cache.version.GridCacheVersion.DR_ID_SHIFT;

/**
 * Entry event order.
 * Two concurrent updates of the same entry can be ordered based on {@link ChangeEventOrder} comparsion.
 * Greater value means that event occurs later.
 */
@IgniteExperimental
public class ChangeEventOrder implements Comparable<ChangeEventOrder>, Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Topology version. */
    private final int topVer;

    /** Node order (used as global order) and DR ID. */
    private final int nodeOrderDrId;

    /** Order. */
    private final long order;

    /** Replica version. */
    private @Nullable ChangeEventOrder otherDcOrder;

    /**
     * @param topVer Topology version plus number of seconds from the start time of the first grid node.
     * @param nodeOrderDrId Node order and DR ID.
     * @param order Version order.
     */
    public ChangeEventOrder(int topVer, int nodeOrderDrId, long order) {
        this.topVer = topVer;
        this.nodeOrderDrId = nodeOrderDrId;
        this.order = order;
    }

    /** @return topVer Topology version plus number of seconds from the start time of the first grid node. */
    public int topVer() {
        return topVer;
    }

    /** @return nodeOrderDrId Node order and DR ID. */
    public int nodeOrderDrId() {
        return nodeOrderDrId;
    }

    /** @return Data center id. */
    public byte dataCenterId() {
        return (byte)((nodeOrderDrId >> DR_ID_SHIFT) & DR_ID_MASK);
    }

    /** @return order Version order. */
    public long order() {
        return order;
    }

    /** @param replicaVer Replication version. */
    public void otherDcOrder(ChangeEventOrder replicaVer) {
        this.otherDcOrder = replicaVer;
    }

    /** @return Replication version. */
    public ChangeEventOrder otherDcOrder() {
        return otherDcOrder;
    }

    /** {@inheritDoc} */
    @Override public int compareTo(@NotNull ChangeEventOrder other) {
        int res = Integer.compare(topVer, other.topVer);

        if (res != 0)
            return res;

        res = Long.compare(order, other.order);

        if (res != 0)
            return res;

        return Integer.compare(nodeOrderDrId, other.nodeOrderDrId);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ChangeEventOrder order1 = (ChangeEventOrder)o;
        return topVer == order1.topVer && nodeOrderDrId == order1.nodeOrderDrId && order == order1.order;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(topVer, nodeOrderDrId, order);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ChangeEventOrder.class, this);
    }
}
