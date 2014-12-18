/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.managers.deployment;

import org.gridgain.grid.util.direct.*;

import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.nio.*;

/**
 * Grid deployment response containing requested resource bytes.
 */
public class GridDeploymentResponse extends GridTcpCommunicationMessageAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /** Result state. */
    private boolean success;

    /** */
    private String errMsg;

    /** Raw class/resource/task. */
    private GridByteArrayList byteSrc;

    /**
     * No-op constructor to support {@link Externalizable} interface.
     * This constructor is not meant to be used for other purposes.
     */
    @SuppressWarnings({"RedundantNoArgConstructor"})
    public GridDeploymentResponse() {
        // No-op.
    }

    /**
     * Sets raw class/resource or serialized task as bytes array.
     *
     * @param byteSrc Class/resource/task source.
     */
    void byteSource(GridByteArrayList byteSrc) {
        this.byteSrc = byteSrc;
    }

    /**
     * Gets raw class/resource or serialized task source as bytes array.
     * @return Class/resource/task source.
     */
    GridByteArrayList byteSource() {
        return byteSrc;
    }

    /**
     * Tests whether corresponding request was processed successful of not.
     *
     * @return {@code true} if request for the source processed
     *      successfully and {@code false} if not.
     */
    boolean success() {
        return success;
    }

    /**
     * Sets corresponding request processing status.
     *
     * @param success {@code true} if request processed successfully and
     *      response keeps source inside and {@code false} otherwise.
     */
    void success(boolean success) {
        this.success = success;
    }

    /**
     * Gets request processing error message. If request processed with error,
     * message will be put in response.
     *
     * @return  Request processing error message.
     */
    String errorMessage() {
        return errMsg;
    }

    /**
     * Sets request processing error message.
     *
     * @param errMsg Request processing error message.
     */
    void errorMessage(String errMsg) {
        this.errMsg = errMsg;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridDeploymentResponse _clone = new GridDeploymentResponse();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        GridDeploymentResponse _clone = (GridDeploymentResponse)_msg;

        _clone.success = success;
        _clone.errMsg = errMsg;
        _clone.byteSrc = byteSrc;
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
                if (!commState.putByteArrayList(null, byteSrc))
                    return false;

                commState.idx++;

            case 1:
                if (!commState.putString(null, errMsg))
                    return false;

                commState.idx++;

            case 2:
                if (!commState.putBoolean(null, success))
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
                GridByteArrayList byteSrc0 = commState.getByteArrayList(null);

                if (byteSrc0 == BYTE_ARR_LIST_NOT_READ)
                    return false;

                byteSrc = byteSrc0;

                commState.idx++;

            case 1:
                String errMsg0 = commState.getString(null);

                if (errMsg0 == STR_NOT_READ)
                    return false;

                errMsg = errMsg0;

                commState.idx++;

            case 2:
                if (buf.remaining() < 1)
                    return false;

                success = commState.getBoolean(null);

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 12;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDeploymentResponse.class, this);
    }
}
