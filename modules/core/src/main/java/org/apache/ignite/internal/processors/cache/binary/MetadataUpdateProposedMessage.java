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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.MarshallableMessage;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.binary.BinaryMetadataHandler;
import org.apache.ignite.internal.managers.communication.ErrorMessage;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.marshaller.Marshaller;
import org.jetbrains.annotations.Nullable;

/**
 * <b>MetadataUpdateProposedMessage</b> and {@link MetadataUpdateAcceptedMessage} messages make a basis for
 * discovery-based protocol for exchanging {@link BinaryMetadata metadata} describing objects in binary format stored in Ignite caches.
 * <p>
 * All interactions with binary metadata are performed through {@link BinaryMetadataHandler}
 * interface implemented in {@link CacheObjectBinaryProcessorImpl} processor.
 * <p>
 * Protocol works as follows:
 * <ol>
 * <li>
 *     Each thread aiming to add/update metadata sends <b>MetadataUpdateProposedMessage</b>
 *     and blocks until receiving acknowledge or reject for proposed update.
 * </li>
 * <li>
 *     Coordinator node checks whether proposed update is in conflict with current version of metadata
 *     for the same typeId.
 *     In case of conflict initial <b>MetadataUpdateProposedMessage</b> is marked rejected and sent to initiator.
 * </li>
 * <li>
 *     If there are no conflicts on coordinator, <b>pending version</b> for metadata of this typeId is bumped up by one;
 *     <b>MetadataUpdateProposedMessage</b> with <b>pending version</b> information is sent across the cluster.
 * </li>
 * <li>
 *     Each node on receiving non-rejected <b>MetadataUpdateProposedMessage</b> updates <b>pending version</b>
 *     for the typeId in metadata local cache.
 * </li>
 * <li>
 *     When <b>MetadataUpdateProposedMessage</b> finishes pass, {@link MetadataUpdateAcceptedMessage ack} is sent.
 *     Ack has the same <b>accepted version</b> as <b>pending version</b>
 *     of initial <b>MetadataUpdateProposedMessage</b> message.
 * </li>
 * <li>
 *     Each node on receiving <b>MetadataUpdateAcceptedMessage</b> updates accepted version for the typeId.
 *     All threads waiting for arrival of ack with this <b>accepted version</b> are unblocked.
 * </li>
 * </ol>
 *
 * If a thread on some node decides to read metadata which has ongoing update
 * (with <b>pending version</b> strictly greater than <b>accepted version</b>)
 * it gets blocked until {@link MetadataUpdateAcceptedMessage} arrives with <b>accepted version</b>
 * equals to <b>pending version</b> of this metadata to the moment when is was initially read by the thread.
 */
public final class MetadataUpdateProposedMessage extends DiscoveryCustomMessage implements MarshallableMessage {
    /** Node UUID which initiated metadata update. */
    @Order(0)
    UUID origNodeId;

    /** */
    private BinaryMetadata metadata;

    /** Serialized {@link #metadata}. */
    @Order(1)
    byte[] metadataBytes;

    /** Metadata type id. */
    @Order(2)
    int typeId;

    /** Metadata version which is pending for update. */
    @Order(3)
    int pendingVer;

    /** Metadata version which is already accepted by entire cluster. */
    @Order(4)
    int acceptedVer;

    /** */
    @Order(5)
    @Nullable ErrorMessage errMsg;

    /** Constructor. */
    public MetadataUpdateProposedMessage() {
        // No-op.
    }

    /**
     * @param metadata   {@link BinaryMetadata} requested to be updated.
     * @param origNodeId ID of node requested update.
     */
    public MetadataUpdateProposedMessage(BinaryMetadata metadata, UUID origNodeId) {
        super(IgniteUuid.randomUuid());

        assert origNodeId != null;
        assert metadata != null;

        this.origNodeId = origNodeId;

        this.metadata = metadata;
        typeId = metadata.typeId();
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryCustomMessage ackMessage() {
        return !rejected() ? new MetadataUpdateAcceptedMessage(typeId, pendingVer) : null;
    }

    /** {@inheritDoc} */
    @Override public boolean isMutable() {
        return true;
    }

    /**
     * @param err Error caused this update to be rejected.
     */
    void markRejected(BinaryObjectException err) {
        errMsg = new ErrorMessage(err);
    }

    /** */
    boolean rejected() {
        return errMsg != null;
    }

    /**
     *
     */
    BinaryObjectException rejectionError() {
        return (BinaryObjectException)ErrorMessage.error(errMsg);
    }

    /** @return Pending version. */
    int pendingVersion() {
        return pendingVer;
    }

    /** @param pendingVer New pending version. */
    void pendingVersion(int pendingVer) {
        this.pendingVer = pendingVer;
    }

    /** */
    int acceptedVersion() {
        return acceptedVer;
    }

    /** @param acceptedVer Accepted version. */
    void acceptedVersion(int acceptedVer) {
        this.acceptedVer = acceptedVer;
    }

    /** */
    UUID origNodeId() {
        return origNodeId;
    }

    /** */
    public BinaryMetadata metadata() {
        return metadata;
    }

    /** @param metadata Metadata. */
    public void metadata(BinaryMetadata metadata) {
        this.metadata = metadata;
    }

    /** */
    public int typeId() {
        return typeId;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(Marshaller marsh) throws IgniteCheckedException {
        if (metadata != null)
            metadataBytes = U.marshal(marsh, metadata);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(Marshaller marsh, ClassLoader ldr) throws IgniteCheckedException {
        if (metadataBytes != null)
            metadata = U.unmarshal(marsh, metadataBytes, ldr);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MetadataUpdateProposedMessage.class, this);
    }
}
