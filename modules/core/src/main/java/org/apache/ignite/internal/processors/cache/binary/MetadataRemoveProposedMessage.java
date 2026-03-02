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
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/**
 * <b>MetadataRemoveProposedMessage</b> and {@link MetadataRemoveAcceptedMessage} messages make a basis for
 * discovery-based protocol for manage {@link BinaryMetadata metadata} describing objects in binary format
 * stored in Ignite caches.
 */
public final class MetadataRemoveProposedMessage implements DiscoveryCustomMessage, Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @Order(0)
    IgniteUuid id;

    /** Node UUID which initiated metadata update. */
    @Order(1)
    UUID origNodeId;

    /** Metadata type id. */
    @Order(2)
    int typeId;

    /** Message acceptance status. */
    @Order(3)
    boolean rejected;

    /** Message received on coordinator. */
    @Order(4)
    boolean onCoordinator = true;

    /** */
    @Order(5)
    String errMsg;

    /** Constructor. */
    public MetadataRemoveProposedMessage() {
        // No-op.
    }

    /**
     * @param typeId Binary type ID.
     * @param origNodeId ID of node requested update.
     */
    public MetadataRemoveProposedMessage(int typeId, UUID origNodeId) {
        assert origNodeId != null;

        id = IgniteUuid.randomUuid();
        this.origNodeId = origNodeId;
        this.typeId = typeId;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid id() {
        return id;
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryCustomMessage ackMessage() {
        return !rejected ? new MetadataRemoveAcceptedMessage(typeId) : null;
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
     * @param errMsg Error message caused this update to be rejected.
     */
    void markRejected(String errMsg) {
        rejected = true;
        this.errMsg = errMsg;
    }

    /** */
    boolean rejected() {
        return rejected;
    }

    /** */
    BinaryObjectException rejectionError() {
        return new BinaryObjectException(errMsg);
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

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MetadataRemoveProposedMessage.class, this);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 503;
    }
}
