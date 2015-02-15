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

import org.apache.ignite.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;

/**
 * Block write request acknowledgement message.
 */
public class IgfsAckMessage extends IgfsCommunicationMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** File id. */
    private IgniteUuid fileId;

    /** Request ID to ack. */
    private long id;

    /** Write exception. */
    @GridDirectTransient
    private IgniteCheckedException err;

    /** */
    private byte[] errBytes;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public IgfsAckMessage() {
        // No-op.
    }

    /**
     * @param fileId File ID.
     * @param id Request ID.
     * @param err Error.
     */
    public IgfsAckMessage(IgniteUuid fileId, long id, @Nullable IgniteCheckedException err) {
        this.fileId = fileId;
        this.id = id;
        this.err = err;
    }

    /**
     * @return File ID.
     */
    public IgniteUuid fileId() {
        return fileId;
    }

    /**
     * @return Batch ID.
     */
    public long id() {
        return id;
    }

    /**
     * @return Error occurred when writing this batch, if any.
     */
    public IgniteCheckedException error() {
        return err;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(Marshaller marsh) throws IgniteCheckedException {
        super.prepareMarshal(marsh);

        if (err != null)
            errBytes = marsh.marshal(err);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(Marshaller marsh, @Nullable ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(marsh, ldr);

        if (errBytes != null)
            err = marsh.unmarshal(errBytes, ldr);
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
                if (!writer.writeField("errBytes", errBytes, MessageFieldType.BYTE_ARR))
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
                errBytes = reader.readField("errBytes", MessageFieldType.BYTE_ARR);

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
        return 64;
    }
}
