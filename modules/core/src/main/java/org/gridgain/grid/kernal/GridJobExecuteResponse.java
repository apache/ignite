/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Job execution response.
 */
public class GridJobExecuteResponse extends GridTcpCommunicationMessageAdapter implements GridTaskMessage {
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
    private GridException gridEx;

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
    private GridException fakeEx;

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
    public GridJobExecuteResponse(UUID nodeId, IgniteUuid sesId, IgniteUuid jobId, byte[] gridExBytes,
        GridException gridEx, byte[] resBytes, Object res, byte[] jobAttrsBytes,
        Map<Object, Object> jobAttrs, boolean isCancelled) {
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
    @Nullable public GridException getException() {
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
    public GridException getFakeException() {
        return fakeEx;
    }

    /**
     * @param fakeEx Fake exception.
     */
    public void setFakeException(GridException fakeEx) {
        this.fakeEx = fakeEx;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridJobExecuteResponse _clone = new GridJobExecuteResponse();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
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
        commState.setBuffer(buf);

        if (!commState.typeWritten) {
            if (!commState.putByte(directType()))
                return false;

            commState.typeWritten = true;
        }

        switch (commState.idx) {
            case 0:
                if (!commState.putByteArray(gridExBytes))
                    return false;

                commState.idx++;

            case 1:
                if (!commState.putBoolean(isCancelled))
                    return false;

                commState.idx++;

            case 2:
                if (!commState.putByteArray(jobAttrsBytes))
                    return false;

                commState.idx++;

            case 3:
                if (!commState.putGridUuid(jobId))
                    return false;

                commState.idx++;

            case 4:
                if (!commState.putUuid(nodeId))
                    return false;

                commState.idx++;

            case 5:
                if (!commState.putByteArray(resBytes))
                    return false;

                commState.idx++;

            case 6:
                if (!commState.putGridUuid(sesId))
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
                byte[] gridExBytes0 = commState.getByteArray();

                if (gridExBytes0 == BYTE_ARR_NOT_READ)
                    return false;

                gridExBytes = gridExBytes0;

                commState.idx++;

            case 1:
                if (buf.remaining() < 1)
                    return false;

                isCancelled = commState.getBoolean();

                commState.idx++;

            case 2:
                byte[] jobAttrsBytes0 = commState.getByteArray();

                if (jobAttrsBytes0 == BYTE_ARR_NOT_READ)
                    return false;

                jobAttrsBytes = jobAttrsBytes0;

                commState.idx++;

            case 3:
                IgniteUuid jobId0 = commState.getGridUuid();

                if (jobId0 == GRID_UUID_NOT_READ)
                    return false;

                jobId = jobId0;

                commState.idx++;

            case 4:
                UUID nodeId0 = commState.getUuid();

                if (nodeId0 == UUID_NOT_READ)
                    return false;

                nodeId = nodeId0;

                commState.idx++;

            case 5:
                byte[] resBytes0 = commState.getByteArray();

                if (resBytes0 == BYTE_ARR_NOT_READ)
                    return false;

                resBytes = resBytes0;

                commState.idx++;

            case 6:
                IgniteUuid sesId0 = commState.getGridUuid();

                if (sesId0 == GRID_UUID_NOT_READ)
                    return false;

                sesId = sesId0;

                commState.idx++;

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
