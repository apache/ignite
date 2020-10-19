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

/**
 *
 */
class ZkDiscoveryNodeLeaveEventData extends ZkDiscoveryEventData {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private long leavedNodeInternalId;

    /** */
    private boolean failed = false;

    /**
     * @param evtId Event ID.
     * @param topVer Topology version.
     * @param leavedNodeInternalId Failed node ID.
     */
    ZkDiscoveryNodeLeaveEventData(long evtId, long topVer, long leavedNodeInternalId) {
       this(evtId, topVer, leavedNodeInternalId, false);
    }

    /**
     * @param evtId Event ID.
     * @param topVer Topology version.
     * @param leavedNodeInternalId Left node ID.
     */
    ZkDiscoveryNodeLeaveEventData(long evtId, long topVer, long leavedNodeInternalId, boolean failed) {
        super(evtId, ZK_EVT_NODE_LEFT, topVer);

        this.leavedNodeInternalId = leavedNodeInternalId;

        this.failed = failed;
    }

    /**
     * @return Left node ID.
     */
    long leavedNodeInternalId() {
        return leavedNodeInternalId;
    }

    /**
     *
     * @return {@code true} if failed.
     */
    boolean failed() {
        return failed;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "ZkDiscoveryNodeLeaveEventData [" +
            "evtId=" + eventId() +
            ", topVer=" + topologyVersion() +
            ", nodeId=" + leavedNodeInternalId +
            ", failed=" + failed + ']';
    }
}
