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

package org.apache.ignite.internal.processors.cache.binary;

import java.util.UUID;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * <b>MetadataRemoveProposedMessage</b> and {@link MetadataRemoveAcceptedMessage} messages make a basis for
 * discovery-based protocol for manage {@link BinaryMetadata metadata} describing objects in binary format
 * stored in Ignite caches.
 */
public final class MetadataRemoveProposedMessage implements DiscoveryCustomMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final IgniteUuid id = IgniteUuid.randomUuid();

    /** Node UUID which initiated metadata update. */
    private final UUID origNodeId;

    /** Metadata type id. */
    private final int typeId;

    /** Message acceptance status. */
    private ProposalStatus status = ProposalStatus.SUCCESSFUL;

    /** Message received on coordinator. */
    private boolean onCoordinator = true;

    /** */
    private BinaryObjectException err;

    /**
     * @param typeId Binary type ID.
     * @param origNodeId ID of node requested update.
     */
    public MetadataRemoveProposedMessage(int typeId, UUID origNodeId) {
        assert origNodeId != null;

        this.origNodeId = origNodeId;

        this.typeId = typeId;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid id() {
        return id;
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryCustomMessage ackMessage() {
        return (status == ProposalStatus.SUCCESSFUL) ? new MetadataRemoveAcceptedMessage(typeId) : null;
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

    /**
     * @param err Error caused this update to be rejected.
     */
    void markRejected(BinaryObjectException err) {
        status = ProposalStatus.REJECTED;
        this.err = err;
    }

    /** */
    boolean rejected() {
        return status == ProposalStatus.REJECTED;
    }

    /** */
    BinaryObjectException rejectionError() {
        return err;
    }

    /** */
    UUID origNodeId() {
        return origNodeId;
    }

    /** */
    public int typeId() {
        return typeId;
    }

    /** */
    public boolean isOnCoordinator() {
        return onCoordinator;
    }

    /** */
    public void setOnCoordinator(boolean onCoordinator) {
        this.onCoordinator = onCoordinator;
    }

    /** Message acceptance status. */
    private enum ProposalStatus {
        /** */
        SUCCESSFUL,

        /** */
        REJECTED
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MetadataRemoveProposedMessage.class, this);
    }
}
