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

import org.apache.ignite.internal.util.direct.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Task session request.
 */
public class GridTaskSessionRequest extends GridTcpCommunicationMessageAdapter implements GridTaskMessage {
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
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridTaskSessionRequest _clone = new GridTaskSessionRequest();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        GridTaskSessionRequest _clone = (GridTaskSessionRequest)_msg;

        _clone.sesId = sesId;
        _clone.jobId = jobId;
        _clone.attrsBytes = attrsBytes;
        _clone.attrs = attrs;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean writeTo(ByteBuffer buf) {
        commState.setBuffer(buf);

        if (!commState.typeWritten) {
            if (!commState.putByte(null, directType()))
                return false;

            commState.typeWritten = true;
        }

        switch (commState.idx) {
            case 0:
                if (!commState.putByteArray("attrsBytes", attrsBytes))
                    return false;

                commState.idx++;

            case 1:
                if (!commState.putGridUuid("jobId", jobId))
                    return false;

                commState.idx++;

            case 2:
                if (!commState.putGridUuid("sesId", sesId))
                    return false;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean readFrom(ByteBuffer buf) {
        commState.setBuffer(buf);

        switch (commState.idx) {
            case 0:
                attrsBytes = commState.getByteArray("attrsBytes");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 1:
                jobId = commState.getGridUuid("jobId");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 2:
                sesId = commState.getGridUuid("sesId");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

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
