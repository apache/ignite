/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.apache.ignite.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.marshaller.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Job siblings response.
 */
public class GridJobSiblingsResponse extends GridTcpCommunicationMessageAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @GridDirectTransient
    private Collection<ComputeJobSibling> siblings;

    /** */
    private byte[] siblingsBytes;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridJobSiblingsResponse() {
        // No-op.
    }

    /**
     * @param siblings Siblings.
     * @param siblingsBytes Serialized siblings.
     */
    public GridJobSiblingsResponse(@Nullable Collection<ComputeJobSibling> siblings, @Nullable byte[] siblingsBytes) {
        this.siblings = siblings;
        this.siblingsBytes = siblingsBytes;
    }

    /**
     * @return Job siblings.
     */
    public Collection<ComputeJobSibling> jobSiblings() {
        return siblings;
    }

    /**
     * @param marsh Marshaller.
     * @throws IgniteCheckedException In case of error.
     */
    public void unmarshalSiblings(IgniteMarshaller marsh) throws IgniteCheckedException {
        assert marsh != null;

        if (siblingsBytes != null)
            siblings = marsh.unmarshal(siblingsBytes, null);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridJobSiblingsResponse _clone = new GridJobSiblingsResponse();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        GridJobSiblingsResponse _clone = (GridJobSiblingsResponse)_msg;

        _clone.siblings = siblings;
        _clone.siblingsBytes = siblingsBytes;
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
                if (!commState.putByteArray(siblingsBytes))
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
                byte[] siblingsBytes0 = commState.getByteArray();

                if (siblingsBytes0 == BYTE_ARR_NOT_READ)
                    return false;

                siblingsBytes = siblingsBytes0;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridJobSiblingsResponse.class, this);
    }
}
