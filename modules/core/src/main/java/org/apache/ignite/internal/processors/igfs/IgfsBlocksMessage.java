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

package org.apache.ignite.internal.processors.igfs;

import org.apache.ignite.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.extensions.communication.*;

import java.io.*;
import java.nio.*;
import java.util.*;

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
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!super.writeTo(buf, writer))
            return false;

        if (!writer.isTypeWritten()) {
            if (!writer.writeMessageType(directType()))
                return false;

            writer.onTypeWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeMapField("blocks", blocks, MessageFieldType.MSG, MessageFieldType.BYTE_ARR))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeField("fileId", fileId, MessageFieldType.IGNITE_UUID))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeField("id", id, MessageFieldType.LONG))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf) {
        reader.setBuffer(buf);

        if (!super.readFrom(buf))
            return false;

        switch (readState) {
            case 0:
                blocks = reader.readMapField("blocks", MessageFieldType.MSG, MessageFieldType.BYTE_ARR, false);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 1:
                fileId = reader.readField("fileId", MessageFieldType.IGNITE_UUID);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 2:
                id = reader.readField("id", MessageFieldType.LONG);

                if (!reader.isLastRead())
                    return false;

                readState++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 66;
    }
}
