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
import java.util.stream.Collectors;
import org.apache.ignite.internal.GridDirectMap;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 *
 */
public class SnapshotRequestMessage extends AbstractSnapshotMessage {
    /** Snapshot request message type (value is {@code 177}). */
    public static final short TYPE_CODE = 177;

    /** Serialization version. */
    private static final long serialVersionUID = 0L;

    /** Map of cache group ids and corresponding set of its partition ids. */
    @GridDirectMap(keyType = Integer.class, valueType = int[].class)
    private Map<Integer, int[]> parts;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public SnapshotRequestMessage() {
        // No-op.
    }

    /**
     * @param snpName Unique snapshot name.
     * @param parts Map of cache group ids and corresponding set of its partition ids to be snapshot.
     */
    public SnapshotRequestMessage(String snpName, Map<Integer, Set<Integer>> parts) {
        super(snpName);

        assert parts != null && !parts.isEmpty();

        this.parts = new HashMap<>();

        for (Map.Entry<Integer, Set<Integer>> e : parts.entrySet())
            this.parts.put(e.getKey(), U.toIntArray(e.getValue()));
    }

    /**
     * @return The demanded cache group partitions per each cache group.
     */
    public Map<Integer, Set<Integer>> parts() {
        Map<Integer, Set<Integer>> res = new HashMap<>();

        for (Map.Entry<Integer, int[]> e : parts.entrySet()) {
            res.put(e.getKey(), e.getValue().length == 0 ? null : Arrays.stream(e.getValue())
                .boxed()
                .collect(Collectors.toSet()));
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!super.writeTo(buf, writer))
            return false;

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        if (writer.state() == 1) {
            if (!writer.writeMap("parts", parts, MessageCollectionItemType.INT, MessageCollectionItemType.INT_ARR))
                return false;

            writer.incrementState();
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        if (!super.readFrom(buf, reader))
            return false;

        if (reader.state() == 1) {
            parts = reader.readMap("parts", MessageCollectionItemType.INT, MessageCollectionItemType.INT_ARR, false);

            if (!reader.isLastRead())
                return false;

            reader.incrementState();
        }

        return reader.afterMessageRead(SnapshotRequestMessage.class);
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SnapshotRequestMessage.class, this, super.toString());
    }
}
