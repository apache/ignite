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

package org.apache.ignite.internal.processors.marshaller;

import java.util.UUID;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
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
public class MappingProposedMessage implements DiscoveryCustomMessage {
    /** */
    private enum ProposalStatus {
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
    private final IgniteUuid id = IgniteUuid.randomUuid();

    /** */
    private final UUID origNodeId;

    /** */
    @GridToStringInclude
    private final MarshallerMappingItem mappingItem;

    /** */
    private ProposalStatus status = ProposalStatus.SUCCESSFUL;

    /** */
    private String conflictingClsName;

    /**
     * @param mappingItem Mapping item.
     * @param origNodeId Orig node id.
     */
    MappingProposedMessage(MarshallerMappingItem mappingItem, UUID origNodeId) {
        assert origNodeId != null;

        this.mappingItem = mappingItem;
        this.origNodeId = origNodeId;
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
            return new MappingAcceptedMessage(mappingItem);
        else
            return null;
    }

    /** {@inheritDoc} */
    @Override public boolean isMutable() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean stopProcess() {
        return false;
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoCache createDiscoCache(GridDiscoveryManager mgr,
        AffinityTopologyVersion topVer, DiscoCache discoCache) {
        throw new UnsupportedOperationException();
    }

    /** */
    MarshallerMappingItem mappingItem() {
        return mappingItem;
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
    @Override public String toString() {
        return S.toString(MappingProposedMessage.class, this);
    }
}
