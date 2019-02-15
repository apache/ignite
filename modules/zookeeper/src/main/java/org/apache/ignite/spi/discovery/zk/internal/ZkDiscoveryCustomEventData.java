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

package org.apache.ignite.spi.discovery.zk.internal;

import java.util.UUID;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;

/**
 *
 */
class ZkDiscoveryCustomEventData extends ZkDiscoveryEventData {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    final long origEvtId;

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
     * @param origEvtId For acknowledge events ID of original event.
     * @param topVer Topology version.
     * @param sndNodeId Sender node ID.
     * @param msg Message instance.
     * @param evtPath Event path.
     */
    ZkDiscoveryCustomEventData(
        long evtId,
        long origEvtId,
        long topVer,
        UUID sndNodeId,
        DiscoverySpiCustomMessage msg,
        String evtPath)
    {
        super(evtId, ZK_EVT_CUSTOM_EVT, topVer);

        assert sndNodeId != null;
        assert msg != null || origEvtId != 0 || !F.isEmpty(evtPath);

        this.origEvtId = origEvtId;
        this.msg = msg;
        this.sndNodeId = sndNodeId;
        this.evtPath = evtPath;
    }

    /**
     * @return {@code True} for custom event ack message.
     */
    boolean ackEvent() {
        return origEvtId != 0;
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
