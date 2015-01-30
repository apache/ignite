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

import org.apache.ignite.lang.*;
import org.apache.ignite.internal.util.direct.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;

/**
 * Job cancellation request.
 */
public class GridJobCancelRequest extends GridTcpCommunicationMessageAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private IgniteUuid sesId;

    /** */
    private IgniteUuid jobId;

    /** */
    private boolean sys;

    /**
     * No-op constructor to support {@link Externalizable} interface.
     * This constructor is not meant to be used for other purposes.
     */
    public GridJobCancelRequest() {
        // No-op.
    }

    /**
     * @param sesId Task session ID.
     */
    public GridJobCancelRequest(IgniteUuid sesId) {
        assert sesId != null;

        this.sesId = sesId;
    }

    /**
     * @param sesId Task session ID.
     * @param jobId Job ID.
     */
    public GridJobCancelRequest(@Nullable IgniteUuid sesId, @Nullable IgniteUuid jobId) {
        assert sesId != null || jobId != null;

        this.sesId = sesId;
        this.jobId = jobId;
    }

    /**
     * @param sesId Session ID.
     * @param jobId Job ID.
     * @param sys System flag.
     */
    public GridJobCancelRequest(@Nullable IgniteUuid sesId, @Nullable IgniteUuid jobId, boolean sys) {
        assert sesId != null || jobId != null;

        this.sesId = sesId;
        this.jobId = jobId;
        this.sys = sys;
    }

    /**
     * Gets execution ID of task to be cancelled.
     *
     * @return Execution ID of task to be cancelled.
     */
    @Nullable public IgniteUuid sessionId() {
        return sesId;
    }

    /**
     * Gets session ID of job to be cancelled. If {@code null}, then
     * all jobs for the specified task execution ID will be cancelled.
     *
     * @return Execution ID of job to be cancelled.
     */
    @Nullable public IgniteUuid jobId() {
        return jobId;
    }

    /**
     * @return {@code True} if request to cancel is sent out of system when task
     *       has already been reduced and further results are no longer interesting.
     */
    public boolean system() {
        return sys;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridJobCancelRequest _clone = new GridJobCancelRequest();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        GridJobCancelRequest _clone = (GridJobCancelRequest)_msg;

        _clone.sesId = sesId;
        _clone.jobId = jobId;
        _clone.sys = sys;
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
                if (!commState.putGridUuid("jobId", jobId))
                    return false;

                commState.idx++;

            case 1:
                if (!commState.putGridUuid("sesId", sesId))
                    return false;

                commState.idx++;

            case 2:
                if (!commState.putBoolean("sys", sys))
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
                jobId = commState.getGridUuid("jobId");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 1:
                sesId = commState.getGridUuid("sesId");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 2:
                sys = commState.getBoolean("sys");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridJobCancelRequest.class, this);
    }
}
