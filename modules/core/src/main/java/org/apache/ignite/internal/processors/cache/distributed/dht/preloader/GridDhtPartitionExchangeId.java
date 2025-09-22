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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.events.DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT;

/**
 * Exchange ID.
 */
public class GridDhtPartitionExchangeId implements Message, Comparable<GridDhtPartitionExchangeId>, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Node ID. */
    @Order(0)
    @GridToStringExclude
    private UUID nodeId;

    /** Event type. */
    @Order(value = 1, method = "event")
    @GridToStringExclude
    private int evt;

    /** Topology version. */
    @Order(value = 2, method = "topologyVersion")
    private AffinityTopologyVersion topVer;

    /** */
    private DiscoveryEvent discoEvt;

    /**
     * @param nodeId Node ID.
     * @param evt Event type.
     * @param topVer Topology version.
     */
    public GridDhtPartitionExchangeId(UUID nodeId, int evt, AffinityTopologyVersion topVer) {
        this.nodeId = nodeId;
        this.evt = evt;
        this.topVer = topVer;
    }

    /**
     * @param nodeId Node ID.
     * @param discoEvt Event.
     * @param topVer Topology version.
     */
    public GridDhtPartitionExchangeId(UUID nodeId, DiscoveryEvent discoEvt, AffinityTopologyVersion topVer) {
        assert nodeId != null;
        assert topVer != null && topVer.topologyVersion() > 0 : topVer;
        assert discoEvt != null;

        this.nodeId = nodeId;
        this.evt = discoEvt.type();
        this.topVer = topVer;
        this.discoEvt = discoEvt;

        assert evt == EVT_NODE_LEFT || evt == EVT_NODE_FAILED || evt == EVT_NODE_JOINED ||
            evt == EVT_DISCOVERY_CUSTOM_EVT;
    }

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridDhtPartitionExchangeId() {
        // No-op.
    }

    /**
     * @return Node ID.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @param nodeId New node ID.
     */
    public void nodeId(UUID nodeId) {
        this.nodeId = nodeId;
    }

    /**
     * @return Event.
     */
    public int event() {
        return evt;
    }

    /**
     * @param evt New event type.
     */
    public void event(int evt) {
        this.evt = evt;
    }

    /**
     * @return Discovery event timestamp.
     */
    long eventTimestamp() {
        assert discoEvt != null;

        return discoEvt.timestamp();
    }

    /**
     * @param discoEvt Discovery event.
     */
    void discoveryEvent(DiscoveryEvent discoEvt) {
        this.discoEvt = discoEvt;
    }

    /**
     * @return Discovery event.
     */
    DiscoveryEvent discoveryEvent() {
        assert discoEvt != null;

        return discoEvt;
    }

    /**
     * @return Discovery event node.
     */
    public ClusterNode eventNode() {
        return discoEvt.eventNode();
    }

    /**
     * @return Discovery event name.
     */
    public String discoveryEventName() {
        return U.gridEventName(evt);
    }

    /**
     * @return Order.
     */
    public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * @param topVer New topology version.
     */
    public void topologyVersion(AffinityTopologyVersion topVer) {
        this.topVer = topVer;
    }

    /**
     * @return {@code True} if exchange is for new node joining.
     */
    public boolean isJoined() {
        return evt == EVT_NODE_JOINED;
    }

    /**
     * @return {@code True} if exchange is for node leaving.
     */
    public boolean isLeft() {
        return evt == EVT_NODE_LEFT || evt == EVT_NODE_FAILED;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeUuid(out, nodeId);
        out.writeObject(topVer);
        out.writeInt(evt);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        nodeId = U.readUuid(in);
        topVer = (AffinityTopologyVersion)in.readObject();
        evt = in.readInt();
    }

    /** {@inheritDoc} */
    @Override public int compareTo(GridDhtPartitionExchangeId o) {
        if (o == this)
            return 0;

        return topVer.compareTo(o.topVer);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = nodeId.hashCode();

        res = 31 * res + evt;
        res = 31 * res + topVer.hashCode();

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (o == null)
            return false;

        if (o == this)
            return true;

        GridDhtPartitionExchangeId id = (GridDhtPartitionExchangeId)o;

        return evt == id.evt && topVer.equals(id.topVer) && nodeId.equals(id.nodeId);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 87;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtPartitionExchangeId.class, this,
            "nodeId", U.id8(nodeId),
            "evt", U.gridEventName(evt));
    }
}
