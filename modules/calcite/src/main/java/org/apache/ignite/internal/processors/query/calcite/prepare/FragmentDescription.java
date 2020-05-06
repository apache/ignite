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

package org.apache.ignite.internal.processors.query.calcite.prepare;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.GridDirectMap;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.query.calcite.message.MarshalableMessage;
import org.apache.ignite.internal.processors.query.calcite.message.MessageType;
import org.apache.ignite.internal.processors.query.calcite.metadata.NodesMapping;
import org.apache.ignite.internal.util.UUIDCollectionMessage;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/** */
public class FragmentDescription implements MarshalableMessage {
    /** */
    private long fragmentId;

    /** */
    private int[] partitions;

    /** */
    private int partitionsCount;

    /** */
    private NodesMapping targetMapping;

    /** */
    @GridDirectTransient
    private Map<Long, List<UUID>> remoteSources;

    /** */
    @GridDirectMap(keyType = Long.class, valueType = Message.class)
    private Map<Long, UUIDCollectionMessage> remoteSources0;

    /** */
    public FragmentDescription() {
    }

    /** */
    public FragmentDescription(long fragmentId, int[] partitions, int partitionsCount,
        NodesMapping targetMapping,
        Map<Long, List<UUID>> remoteSources) {
        this.fragmentId = fragmentId;
        this.partitions = partitions;
        this.partitionsCount = partitionsCount;
        this.targetMapping = targetMapping;
        this.remoteSources = remoteSources;
    }

    /** */
    public long fragmentId() {
        return fragmentId;
    }

    /** */
    public NodesMapping targetMapping() {
        return targetMapping;
    }

    /** */
    public Map<Long, List<UUID>> remoteSources() {
        return remoteSources;
    }

    /** */
    public int[] partitions() {
        return partitions;
    }

    /** */
    public int partitionsCount() {
        return partitionsCount;
    }

    /** {@inheritDoc} */
    @Override public MessageType type() {
        return MessageType.FRAGMENT_DESCRIPTION;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeLong("fragmentId", fragmentId))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeIntArray("partitions", partitions))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeInt("partitionsCount", partitionsCount))
                    return false;

                writer.incrementState();

            case 3:
                if (!writer.writeMap("remoteSources0", remoteSources0, MessageCollectionItemType.LONG, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeMessage("targetMapping", targetMapping))
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

        switch (reader.state()) {
            case 0:
                fragmentId = reader.readLong("fragmentId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                partitions = reader.readIntArray("partitions");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                partitionsCount = reader.readInt("partitionsCount");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 3:
                remoteSources0 = reader.readMap("remoteSources0", MessageCollectionItemType.LONG, MessageCollectionItemType.MSG, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                targetMapping = reader.readMessage("targetMapping");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(FragmentDescription.class);
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 5;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(Marshaller marshaller) {
        if (remoteSources0 == null && remoteSources != null) {
            remoteSources0 = U.newHashMap(remoteSources.size());

            for (Map.Entry<Long, List<UUID>> e : remoteSources.entrySet())
                remoteSources0.put(e.getKey(), new UUIDCollectionMessage(e.getValue()));
        }

        if (targetMapping != null)
            targetMapping.prepareMarshal(marshaller);
    }

    /** {@inheritDoc} */
    @Override public void prepareUnmarshal(Marshaller marshaller, ClassLoader loader) {
        if (remoteSources == null && remoteSources0 != null) {
            remoteSources = U.newHashMap(remoteSources0.size());

            for (Map.Entry<Long, UUIDCollectionMessage> e : remoteSources0.entrySet())
                remoteSources.put(e.getKey(), new ArrayList<>(e.getValue().uuids()));
        }

        if (targetMapping != null)
            targetMapping.prepareUnmarshal(marshaller, loader);
    }
}
