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

package org.apache.ignite.spi.discovery.zk.internal;

import java.util.UUID;
import org.apache.ignite.internal.events.DiscoveryCustomEvent;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;

/**
 *
 */
class ZkDiscoveryCustomEventData extends ZkDiscoveryEventData {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final int CUSTOM_MSG_ACK_FLAG = 0x01;

    /** */
    final UUID sndNodeId;

    /** */
    final String evtPath;

    /** Message instance (can be marshalled as part of ZkDiscoveryCustomEventData or stored in separate znode. */
    DiscoverySpiCustomMessage msg;

    /** Unmarshalled message. */
    transient DiscoverySpiCustomMessage resolvedMsg;

    /**
     * @param evtId Event ID.
     * @param topVer Topology version.
     * @param sndNodeId Sender node ID.
     * @param msg Message instance.
     * @param evtPath Event path.
     * @param ack Acknowledge event flag.
     */
    ZkDiscoveryCustomEventData(long evtId,
        long topVer,
        UUID sndNodeId,
        DiscoverySpiCustomMessage msg,
        String evtPath,
        boolean ack)
    {
        super(evtId, DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT, topVer);

        assert sndNodeId != null;
        assert msg != null || ack || !F.isEmpty(evtPath);

        this.msg = msg;
        this.sndNodeId = sndNodeId;
        this.evtPath = evtPath;

        if (ack)
            flags |= CUSTOM_MSG_ACK_FLAG;
    }

    /**
     * @return {@code True} for custom event ack message.
     */
    boolean ackEvent() {
        return flagSet(CUSTOM_MSG_ACK_FLAG);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "ZkDiscoveryCustomEventData [" +
            "evtId=" + eventId() +
            ", topVer=" + topologyVersion() +
            ", sndNode=" + sndNodeId +
            ", ack=" + ackEvent() +
            ']';
    }
}
