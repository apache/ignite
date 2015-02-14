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

import org.apache.ignite.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Job execution response.
 */
public class GridJobExecuteResponse extends MessageAdapter implements GridTaskMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private UUID nodeId;

    /** */
    private IgniteUuid sesId;

    /** */
    private IgniteUuid jobId;

    /** */
    private byte[] gridExBytes;

    /** */
    @GridDirectTransient
    private IgniteException gridEx;

    /** */
    private byte[] resBytes;

    /** */
    @GridDirectTransient
    private Object res;

    /** */
    private byte[] jobAttrsBytes;

    /** */
    @GridDirectTransient
    private Map<Object, Object> jobAttrs;

    /** */
    private boolean isCancelled;

    /** */
    @GridToStringExclude
    @GridDirectTransient
    private IgniteException fakeEx;

    /**
     * No-op constructor to support {@link Externalizable} interface. This
     * constructor is not meant to be used for other purposes.
     */
    public GridJobExecuteResponse() {
        // No-op.
    }

    /**
     * @param nodeId Sender node ID.
     * @param sesId Task session ID
     * @param jobId Job ID.
     * @param gridExBytes Serialized grid exception.
     * @param gridEx Grid exception.
     * @param resBytes Serialized result.
     * @param res Result.
     * @param jobAttrsBytes Serialized job attributes.
     * @param jobAttrs Job attributes.
     * @param isCancelled Whether job was cancelled or not.
     */
    public GridJobExecuteResponse(UUID nodeId,
        IgniteUuid sesId,
        IgniteUuid jobId,
        byte[] gridExBytes,
        IgniteException gridEx,
        byte[] resBytes,
        Object res,
        byte[] jobAttrsBytes,
        Map<Object, Object> jobAttrs,
        boolean isCancelled)
    {
        assert nodeId != null;
        assert sesId != null;
        assert jobId != null;

        this.nodeId = nodeId;
        this.sesId = sesId;
        this.jobId = jobId;
        this.gridExBytes = gridExBytes;
        this.gridEx = gridEx;
        this.resBytes = resBytes;
        this.res = res;
        this.jobAttrsBytes = jobAttrsBytes;
        this.jobAttrs = jobAttrs;
        this.isCancelled = isCancelled;
    }

    /**
     * @return Task session ID.
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

    /**
     * @return Serialized job result.
     */
    @Nullable public byte[] getJobResultBytes() {
        return resBytes;
    }

    /**
     * @return Job result.
     */
    @Nullable public Object getJobResult() {
        return res;
    }

    /**
     * @return Serialized job exception.
     */
    @Nullable public byte[] getExceptionBytes() {
        return gridExBytes;
    }

    /**
     * @return Job exception.
     */
    @Nullable public IgniteException getException() {
        return gridEx;
    }

    /**
     * @return Serialized job attributes.
     */
    @Nullable public byte[] getJobAttributesBytes() {
        return jobAttrsBytes;
    }

    /**
     * @return Job attributes.
     */
    @Nullable public Map<Object, Object> getJobAttributes() {
        return jobAttrs;
    }

    /**
     * @return Job cancellation status.
     */
    public boolean isCancelled() {
        return isCancelled;
    }

    /**
     * @return Sender node ID.
     */
    public UUID getNodeId() {
        return nodeId;
    }

    /**
     * @return Fake exception.
     */
    public IgniteException getFakeException() {
        return fakeEx;
    }

    /**
     * @param fakeEx Fake exception.
     */
    public void setFakeException(IgniteException fakeEx) {
        this.fakeEx = fakeEx;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public MessageAdapter clone() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override protected void clone0(MessageAdapter _msg) {
        GridJobExecuteResponse _clone = (GridJobExecuteResponse)_msg;

        _clone.nodeId = nodeId;
        _clone.sesId = sesId;
        _clone.jobId = jobId;
        _clone.gridExBytes = gridExBytes;
        _clone.gridEx = gridEx;
        _clone.resBytes = resBytes;
        _clone.res = res;
        _clone.jobAttrsBytes = jobAttrsBytes;
        _clone.jobAttrs = jobAttrs;
        _clone.isCancelled = isCancelled;
        _clone.fakeEx = fakeEx;
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
                if (!writer.writeByteArray("gridExBytes", gridExBytes))
                    return false;

                state.increment();

            case 1:
                if (!writer.writeBoolean("isCancelled", isCancelled))
                    return false;

                state.increment();

            case 2:
                if (!writer.writeByteArray("jobAttrsBytes", jobAttrsBytes))
                    return false;

                state.increment();

            case 3:
                if (!writer.writeIgniteUuid("jobId", jobId))
                    return false;

                state.increment();

            case 4:
                if (!writer.writeUuid("nodeId", nodeId))
                    return false;

                state.increment();

            case 5:
                if (!writer.writeByteArray("resBytes", resBytes))
                    return false;

                state.increment();

            case 6:
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
                gridExBytes = reader.readByteArray("gridExBytes");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 1:
                isCancelled = reader.readBoolean("isCancelled");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 2:
                jobAttrsBytes = reader.readByteArray("jobAttrsBytes");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 3:
                jobId = reader.readIgniteUuid("jobId");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 4:
                nodeId = reader.readUuid("nodeId");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 5:
                resBytes = reader.readByteArray("resBytes");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 6:
                sesId = reader.readIgniteUuid("sesId");

                if (!reader.isLastRead())
                    return false;

                readState++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridJobExecuteResponse.class, this);
    }
}
