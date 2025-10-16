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

import java.io.Serializable;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * Wrapper for {@link BinaryMetadata} which is stored in metadata local cache on each node.
 * Used internally to track version counters (see javadoc for {@link MetadataUpdateProposedMessage} for more details).
 * The version refers solely to the internal protocol for updating BinaryMetadata and is unknown externally.
 * It can be updated dynamically from different nodes and threads on the same node.
 */
public final class BinaryMetadataVersionInfo implements Serializable, Message {
    /** Type code. */
    public static final short TYPE_CODE = 505;

    /** */
    private static final long serialVersionUID = 0L;

    /** The actual binary metadata. */
    private BinaryMetadata metadata;

    /** Serialized binary metadata. */
    @Order(0)
    private byte[] metadataBytes;

    /**
     * The version of metadata that has been proposed for update. This represents how many unique updates have been issued
     * for this type. When a metadata update is proposed, this version is incremented.
     */
    @Order(value = 1, method = "pendingVersion")
    private int pendingVer;

    /**
     * The version of metadata that has been accepted by the entire cluster.
     * This represents the number of updates that have been confirmed across all nodes.
     */
    @Order(value = 2, method = "acceptedVersion")
    private int acceptedVer;

    /** A flag indicating whether the metadata is currently being removed. */
    @Order(3)
    private transient boolean removing;

    /** */
    public BinaryMetadataVersionInfo() {
        // No-op.
    }

    /**
     * @param metadata Metadata.
     */
    BinaryMetadataVersionInfo(BinaryMetadata metadata) {
        this(metadata, 0, 0, false);
    }

    /**
     * @param metadata Metadata.
     * @param pendingVer Version of this metadata - how many updates were issued for this type.
     * @param acceptedVer Pending updates count.
     */
    BinaryMetadataVersionInfo(BinaryMetadata metadata, int pendingVer, int acceptedVer) {
        this(metadata, pendingVer, acceptedVer, false);
    }

    /**
     * @param metadata Metadata.
     * @param pendingVer Pending updates count.
     * @param acceptedVer Version of this metadata - how many updates were issued for this type.
     * @param removing Flag means the metadata is removing now.
     */
    private BinaryMetadataVersionInfo(BinaryMetadata metadata, int pendingVer, int acceptedVer, boolean removing) {
        assert metadata != null;

        this.metadata = metadata;
        this.pendingVer = pendingVer;
        this.acceptedVer = acceptedVer;
        this.removing = removing;
    }

    /**
     * @return Metadata version info with remove state where remove pending message has been handled.
     */
    BinaryMetadataVersionInfo createRemoving() {
        return new BinaryMetadataVersionInfo(metadata, pendingVer, acceptedVer, true);
    }

    /**
     * @return Binary metadata.
     */
    BinaryMetadata metadata() {
        return metadata;
    }

    /**
     * @return The version of metadata that has been proposed for update.
     */
    public int pendingVersion() {
        return pendingVer;
    }

    /**
     * @param pendingVer The version of metadata that has been proposed for update.
     */
    public void pendingVersion(int pendingVer) {
        this.pendingVer = pendingVer;
    }

    /**
     * @return The version of metadata that has been accepted by the entire cluster.
     */
    public int acceptedVersion() {
        return acceptedVer;
    }

    /**
     * @param acceptedVer The version of metadata that has been accepted by the entire cluster.
     */
    public void acceptedVersion(int acceptedVer) {
        this.acceptedVer = acceptedVer;
    }

    /**
     * @return {@code true} is the metadata is removing now.
     */
    public boolean removing() {
        return removing;
    }

    /**
     * @param removing {@code true} is the metadata is removing now.
     */
    public void removing(boolean removing) {
        this.removing = removing;
    }

    /**
     * @return Serialized binary metadata.
     */
    public byte[] metadataBytes() {
        return metadataBytes;
    }

    /**
     * @param metadataBytes Serialized binary metadata.
     */
    public void metadataBytes(byte[] metadataBytes) {
        this.metadataBytes = metadataBytes;
    }

    /**
     * @param ctx Grid kernal context.
     * @throws IgniteCheckedException If failed.
     */
    public void marshalMetadata(GridKernalContext ctx) throws IgniteCheckedException {
        if (metadataBytes == null)
            metadataBytes = U.marshal(ctx, metadata);
    }

    /**
     * @param ctx Grid kernal context.
     * @throws IgniteCheckedException If failed.
     */
    public void unmarshalMetadata(GridKernalContext ctx) throws IgniteCheckedException {
        if (metadata == null && metadataBytes != null) {
            metadata = U.unmarshal(ctx, metadataBytes, U.resolveClassLoader(ctx.config()));

            metadataBytes = null;
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "[typeId=" + metadata.typeId() +
            ", pendingVer=" + pendingVer +
            ", acceptedVer=" + acceptedVer +
            ", removing=" + removing +
            "]";
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }
}
