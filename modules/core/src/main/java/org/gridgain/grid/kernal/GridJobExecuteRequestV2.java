/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.apache.ignite.compute.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.direct.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Updated job execute request with subject ID.
 */
public class GridJobExecuteRequestV2 extends GridJobExecuteRequest {
    /** */
    private static final long serialVersionUID = -1470089047880101067L;

    /** Subject ID. */
    private UUID subjId;

    /**
     * No-op constructor to support {@link Externalizable} interface.
     */
    public GridJobExecuteRequestV2() {
        // No-op.
    }

    /**
     * @param sesId Task session ID.
     * @param jobId Job ID.
     * @param taskName Task name.
     * @param userVer Code version.
     * @param taskClsName Fully qualified task name.
     * @param jobBytes Job serialized body.
     * @param job Job.
     * @param startTaskTime Task execution start time.
     * @param timeout Task execution timeout.
     * @param top Topology.
     * @param siblingsBytes Serialized collection of split siblings.
     * @param siblings Collection of split siblings.
     * @param sesAttrsBytes Map of session attributes.
     * @param sesAttrs Session attributes.
     * @param jobAttrsBytes Job context attributes.
     * @param jobAttrs Job attributes.
     * @param cpSpi Collision SPI.
     * @param clsLdrId Task local class loader id.
     * @param depMode Task deployment mode.
     * @param dynamicSiblings {@code True} if siblings are dynamic.
     * @param ldrParticipants Other node class loader IDs that can also load classes.
     * @param forceLocDep {@code True} If remote node should ignore deployment settings.
     * @param sesFullSup {@code True} if session attributes are disabled.
     * @param internal {@code True} if internal job.
     * @param subjId Subject ID.
     */
    public GridJobExecuteRequestV2(
        IgniteUuid sesId,
        IgniteUuid jobId,
        String taskName,
        String userVer,
        String taskClsName,
        byte[] jobBytes,
        GridComputeJob job,
        long startTaskTime,
        long timeout,
        @Nullable Collection<UUID> top,
        byte[] siblingsBytes,
        Collection<GridComputeJobSibling> siblings,
        byte[] sesAttrsBytes,
        Map<Object, Object> sesAttrs,
        byte[] jobAttrsBytes,
        Map<? extends Serializable, ? extends Serializable> jobAttrs,
        String cpSpi,
        IgniteUuid clsLdrId,
        GridDeploymentMode depMode,
        boolean dynamicSiblings,
        Map<UUID, IgniteUuid> ldrParticipants,
        boolean forceLocDep,
        boolean sesFullSup,
        boolean internal,
        UUID subjId
    ) {
        super(sesId, jobId, taskName, userVer, taskClsName, jobBytes, job, startTaskTime, timeout, top, siblingsBytes,
            siblings, sesAttrsBytes, sesAttrs, jobAttrsBytes, jobAttrs, cpSpi, clsLdrId, depMode, dynamicSiblings,
            ldrParticipants, forceLocDep, sesFullSup, internal);

        this.subjId = subjId;
    }

    /** {@inheritDoc} */
    @Override public UUID getSubjectId() {
        return subjId;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridJobExecuteRequestV2 _clone = new GridJobExecuteRequestV2();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        super.clone0(_msg);

        GridJobExecuteRequestV2 _clone = (GridJobExecuteRequestV2)_msg;

        _clone.subjId = subjId;
    }

    /** {@inheritDoc} */
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
            case 21:
                if (!commState.putUuid(subjId))
                    return false;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf) {
        commState.setBuffer(buf);

        if (!super.readFrom(buf))
            return false;

        switch (commState.idx) {
            case 21:
                UUID subjId0 = commState.getUuid();

                if (subjId0 == UUID_NOT_READ)
                    return false;

                subjId = subjId0;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 81;
    }
}
