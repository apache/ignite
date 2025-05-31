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
 *
 */

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.io.Externalizable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.internal.GridDirectMap;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class SnapshotFilesRequestMessage extends AbstractSnapshotMessage {
    /** Snapshot request message type (value is {@code 178}). */
    public static final short TYPE_CODE = 178;

    /** Serialization version. */
    private static final long serialVersionUID = 0L;

    /** Snapshot operation request ID. */
    private UUID reqId;

    /** Snapshot name to request. */
    private String snpName;

    /** Snapshot directory path. */
    private String snpPath;

    /** Map of cache group ids and corresponding set of its partition ids. */
    @GridDirectMap(keyType = Integer.class, valueType = int[].class)
    private Map<Integer, int[]> parts;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public SnapshotFilesRequestMessage() {
        // No-op.
    }

    /**
     * @param msgId Unique message id.
     * @param reqId Snapshot operation request ID.
     * @param snpName Snapshot name to request.
     * @param snpPath Snapshot directory path.
     * @param parts Map of cache group ids and corresponding set of its partition ids to be snapshot.
     */
    public SnapshotFilesRequestMessage(
        String msgId,
        UUID reqId,
        String snpName,
        @Nullable String snpPath,
        Map<Integer, Set<Integer>> parts
    ) {
        super(msgId);

        assert parts != null && !parts.isEmpty();

        this.reqId = reqId;
        this.snpName = snpName;
        this.snpPath = snpPath;
        this.parts = new HashMap<>();

        for (Map.Entry<Integer, Set<Integer>> e : F.view(parts.entrySet(), e -> !F.isEmpty(e.getValue())))
            this.parts.put(e.getKey(), U.toIntArray(e.getValue()));
    }

    /**
     * @return The demanded cache group partitions per each cache group.
     */
    public Map<Integer, Set<Integer>> parts() {
        Map<Integer, Set<Integer>> res = new HashMap<>();

        for (Map.Entry<Integer, int[]> e : parts.entrySet())
            res.put(e.getKey(), Arrays.stream(e.getValue()).boxed().collect(Collectors.toSet()));

        return res;
    }

    /**
     * @return Requested snapshot name.
     */
    public String snapshotName() {
        return snpName;
    }

    /**
     * @return Snapshot directory path.
     */
    public String snapshotPath() {
        return snpPath;
    }

    /**
     * @return Snapshot operation request ID.
     */
    public UUID requestId() {
        return reqId;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!super.writeTo(buf, writer))
            return false;

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 1:
                if (!writer.writeMap(parts, MessageCollectionItemType.INT, MessageCollectionItemType.INT_ARR))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeUuid(reqId))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeString(snpName))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeString(snpPath))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!super.readFrom(buf, reader))
            return false;

        switch (reader.state()) {
            case 1:
                parts = reader.readMap(MessageCollectionItemType.INT, MessageCollectionItemType.INT_ARR, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                reqId = reader.readUuid();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                snpName = reader.readString();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                snpPath = reader.readString();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SnapshotFilesRequestMessage.class, this, super.toString());
    }
}
