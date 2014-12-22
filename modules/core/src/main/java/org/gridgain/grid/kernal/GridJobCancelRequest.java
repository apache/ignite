/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.apache.ignite.lang.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.typedef.internal.*;
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
                IgniteUuid jobId0 = commState.getGridUuid("jobId");

                if (jobId0 == GRID_UUID_NOT_READ)
                    return false;

                jobId = jobId0;

                commState.idx++;

            case 1:
                IgniteUuid sesId0 = commState.getGridUuid("sesId");

                if (sesId0 == GRID_UUID_NOT_READ)
                    return false;

                sesId = sesId0;

                commState.idx++;

            case 2:
                if (buf.remaining() < 1)
                    return false;

                sys = commState.getBoolean("sys");

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
