/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.igfs;

import java.io.Externalizable;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.ignite.internal.GridDirectMap;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * IGFS write blocks message.
 */
public class IgfsBlocksMessage extends IgfsCommunicationMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** File id. */
    private IgniteUuid fileId;

    /** Batch id */
    private long id;

    /** Blocks to store. */
    @GridDirectMap(keyType = IgfsBlockKey.class, valueType = byte[].class)
    private Map<IgfsBlockKey, byte[]> blocks;

    /**
     * Empty constructor required by {@link Externalizable}
     */
    public IgfsBlocksMessage() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param fileId File ID.
     * @param id Message id.
     * @param blocks Blocks to put in cache.
     */
    public IgfsBlocksMessage(IgniteUuid fileId, long id, Map<IgfsBlockKey, byte[]> blocks) {
        this.fileId = fileId;
        this.id = id;
        this.blocks = blocks;
    }

    /**
     * @return File id.
     */
    public IgniteUuid fileId() {
        return fileId;
    }

    /**
     * @return Batch id.
     */
    public long id() {
        return id;
    }

    /**
     * @return Map of blocks to put in cache.
     */
    public Map<IgfsBlockKey, byte[]> blocks() {
        return blocks;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
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

        switch (writer.state()) {
            case 0:
                if (!writer.writeMap("blocks", blocks, MessageCollectionItemType.MSG, MessageCollectionItemType.BYTE_ARR))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeIgniteUuid("fileId", fileId))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeLong("id", id))
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

        switch (reader.state()) {
            case 0:
                blocks = reader.readMap("blocks", MessageCollectionItemType.MSG, MessageCollectionItemType.BYTE_ARR, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                fileId = reader.readIgniteUuid("fileId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                id = reader.readLong("id");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(IgfsBlocksMessage.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 66;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 3;
    }
}
