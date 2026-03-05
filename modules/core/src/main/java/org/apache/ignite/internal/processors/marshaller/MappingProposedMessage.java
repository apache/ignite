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

package org.apache.ignite.internal.processors.marshaller;

import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/**
 * Node sends this message when it wants to propose new marshaller mapping and to ensure that there are no conflicts
 * with this mapping on other nodes in cluster.
 *
 * After sending this message to the cluster sending node gets blocked until mapping is either accepted or rejected.
 *
 * When it completes a pass around the cluster ring with no conflicts observed,
 * {@link MappingAcceptedMessage} is sent as an acknowledgement that everything is fine.
 */
public class MappingProposedMessage implements DiscoveryCustomMessage, Message {
    /** */
    enum ProposalStatus {
        /** */
        SUCCESSFUL,
        /** */
        IN_CONFLICT,
        /** */
        DUPLICATED
    }

    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @Order(0)
    IgniteUuid id;

    /** */
    @Order(1)
    UUID origNodeId;

    /** */
    @Order(2)
    byte platformId;

    /** */
    @Order(3)
    int typeId;

    /** */
    @Order(4)
    String clsName;

    /** */
    @Order(5)
    ProposalStatus status;

    /** */
    @Order(6)
    String conflictingClsName;

    /** */
    public MappingProposedMessage() {
        // No-op.
    }

    /**
     * @param mappingItem Mapping item.
     * @param origNodeId Orig node id.
     */
    MappingProposedMessage(MarshallerMappingItem mappingItem, UUID origNodeId) {
        assert origNodeId != null;

        this.id = IgniteUuid.randomUuid();
        this.platformId = mappingItem.platformId();
        this.typeId = mappingItem.typeId();
        this.clsName = mappingItem.className();
        this.origNodeId = origNodeId;
        this.status = ProposalStatus.SUCCESSFUL;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid id() {
        return id;
    }

    /**
     * {@inheritDoc}
     */
    @Nullable @Override public DiscoveryCustomMessage ackMessage() {
        if (status == ProposalStatus.SUCCESSFUL)
            return new MappingAcceptedMessage(mappingItem());
        else
            return null;
    }

    /** {@inheritDoc} */
    @Override public boolean isMutable() {
        return true;
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoCache createDiscoCache(GridDiscoveryManager mgr,
        AffinityTopologyVersion topVer, DiscoCache discoCache) {
        throw new UnsupportedOperationException();
    }

    /** */
    MarshallerMappingItem mappingItem() {
        return new MarshallerMappingItem(platformId, typeId, clsName);
    }

    /** */
    UUID origNodeId() {
        return origNodeId;
    }

    /** */
    boolean inConflict() {
        return status == ProposalStatus.IN_CONFLICT;
    }

    /** */
    public boolean duplicated() {
        return status == ProposalStatus.DUPLICATED;
    }

    /** */
    void conflictingWithClass(String conflClsName) {
        status = ProposalStatus.IN_CONFLICT;
        conflictingClsName = conflClsName;
    }

    /** */
    void markDuplicated() {
        status = ProposalStatus.DUPLICATED;
    }

    /** */
    String conflictingClassName() {
        return conflictingClsName;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 512;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MappingProposedMessage.class, this);
    }
}
