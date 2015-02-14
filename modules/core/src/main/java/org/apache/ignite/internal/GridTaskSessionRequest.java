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

package org.apache.ignite.internal;

import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.extensions.communication.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Task session request.
 */
public class GridTaskSessionRequest extends MessageAdapter implements GridTaskMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Task session ID. */
    private IgniteUuid sesId;

    /** ID of job within a task. */
    private IgniteUuid jobId;

    /** Changed attributes bytes. */
    private byte[] attrsBytes;

    /** Changed attributes. */
    @GridDirectTransient
    private Map<?, ?> attrs;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridTaskSessionRequest() {
        // No-op.
    }

    /**
     * @param sesId Session ID.
     * @param jobId Job ID.
     * @param attrsBytes Serialized attributes.
     * @param attrs Attributes.
     */
    public GridTaskSessionRequest(IgniteUuid sesId, IgniteUuid jobId, byte[] attrsBytes, Map<?, ?> attrs) {
        assert sesId != null;
        assert attrsBytes != null;
        assert attrs != null;

        this.sesId = sesId;
        this.jobId = jobId;
        this.attrsBytes = attrsBytes;
        this.attrs = attrs;
    }

    /**
     * @return Changed attributes (serialized).
     */
    public byte[] getAttributesBytes() {
        return attrsBytes;
    }

    /**
     * @return Changed attributes.
     */
    public Map<?, ?> getAttributes() {
        return attrs;
    }

    /**
     * @return Session ID.
     */
    @Override public IgniteUuid getSessionId() {
        return sesId;
    }

    /**
     * @return Job ID.
     */
    public IgniteUuid getJobId() {
        return jobId;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public MessageAdapter clone() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override protected void clone0(MessageAdapter _msg) {
        GridTaskSessionRequest _clone = (GridTaskSessionRequest)_msg;

        _clone.sesId = sesId;
        _clone.jobId = jobId;
        _clone.attrsBytes = attrsBytes;
        _clone.attrs = attrs;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean writeTo(ByteBuffer buf) {
        MessageWriteState state = MessageWriteState.get();
        MessageWriter writer = state.writer();

        writer.setBuffer(buf);

        if (!state.isTypeWritten()) {
            if (!writer.writeByte(null, directType()))
                return false;

            state.setTypeWritten();
        }

        switch (state.index()) {
            case 0:
                if (!writer.writeByteArray("attrsBytes", attrsBytes))
                    return false;

                state.increment();

            case 1:
                if (!writer.writeIgniteUuid("jobId", jobId))
                    return false;

                state.increment();

            case 2:
                if (!writer.writeIgniteUuid("sesId", sesId))
                    return false;

                state.increment();

        }

        return true;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean readFrom(ByteBuffer buf) {
        reader.setBuffer(buf);

        switch (readState) {
            case 0:
                attrsBytes = reader.readByteArray("attrsBytes");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 1:
                jobId = reader.readIgniteUuid("jobId");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 2:
                sesId = reader.readIgniteUuid("sesId");

                if (!reader.isLastRead())
                    return false;

                readState++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 6;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridTaskSessionRequest.class, this);
    }
}
