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

package org.apache.ignite.internal.processors.fs;

import org.apache.ignite.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.direct.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;

/**
 * Block write request acknowledgement message.
 */
public class GridGgfsAckMessage extends GridGgfsCommunicationMessage {
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
    public GridGgfsAckMessage() {
        // No-op.
    }

    /**
     * @param fileId File ID.
     * @param id Request ID.
     * @param err Error.
     */
    public GridGgfsAckMessage(IgniteUuid fileId, long id, @Nullable IgniteCheckedException err) {
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
    @Override public void prepareMarshal(IgniteMarshaller marsh) throws IgniteCheckedException {
        super.prepareMarshal(marsh);

        if (err != null)
            errBytes = marsh.marshal(err);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(IgniteMarshaller marsh, @Nullable ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(marsh, ldr);

        if (errBytes != null)
            err = marsh.unmarshal(errBytes, ldr);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridGgfsAckMessage _clone = new GridGgfsAckMessage();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        super.clone0(_msg);

        GridGgfsAckMessage _clone = (GridGgfsAckMessage)_msg;

        _clone.fileId = fileId;
        _clone.id = id;
        _clone.err = err;
        _clone.errBytes = errBytes;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean writeTo(ByteBuffer buf) {
        commState.setBuffer(buf);

        if (!super.writeTo(buf))
            return false;

        if (!commState.typeWritten) {
            if (!commState.putByte(directType()))
                return false;

            commState.typeWritten = true;
        }

        switch (commState.idx) {
            case 0:
                if (!commState.putByteArray(errBytes))
                    return false;

                commState.idx++;

            case 1:
                if (!commState.putGridUuid(fileId))
                    return false;

                commState.idx++;

            case 2:
                if (!commState.putLong(id))
                    return false;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean readFrom(ByteBuffer buf) {
        commState.setBuffer(buf);

        if (!super.readFrom(buf))
            return false;

        switch (commState.idx) {
            case 0:
                byte[] errBytes0 = commState.getByteArray();

                if (errBytes0 == BYTE_ARR_NOT_READ)
                    return false;

                errBytes = errBytes0;

                commState.idx++;

            case 1:
                IgniteUuid fileId0 = commState.getGridUuid();

                if (fileId0 == GRID_UUID_NOT_READ)
                    return false;

                fileId = fileId0;

                commState.idx++;

            case 2:
                if (buf.remaining() < 8)
                    return false;

                id = commState.getLong();

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 65;
    }
}
