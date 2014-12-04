/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.ggfs;

import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.marshaller.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;

/**
 * Indicates that entry scheduled for delete was actually deleted.
 */
public class GridGgfsDeleteMessage extends GridGgfsCommunicationMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Deleted entry ID. */
    private IgniteUuid id;

    /** Optional error. */
    @GridDirectTransient
    private GridException err;

    /** */
    private byte[] errBytes;

    /**
     * {@link Externalizable} support.
     */
    public GridGgfsDeleteMessage() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param id Deleted entry ID.
     */
    public GridGgfsDeleteMessage(IgniteUuid id) {
        assert id != null;

        this.id = id;
    }

    /**
     * Constructor.
     *
     * @param id Entry ID.
     * @param err Error.
     */
    public GridGgfsDeleteMessage(IgniteUuid id, GridException err) {
        assert err != null;

        this.id = id;
        this.err = err;
    }

    /**
     * @return Deleted entry ID.
     */
    public IgniteUuid id() {
        return id;
    }

    /**
     * @return Error.
     */
    public GridException error() {
        return err;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridMarshaller marsh) throws GridException {
        super.prepareMarshal(marsh);

        if (err != null)
            errBytes = marsh.marshal(err);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridMarshaller marsh, @Nullable ClassLoader ldr) throws GridException {
        super.finishUnmarshal(marsh, ldr);

        if (errBytes != null)
            err = marsh.unmarshal(errBytes, ldr);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridGgfsDeleteMessage _clone = new GridGgfsDeleteMessage();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        super.clone0(_msg);

        GridGgfsDeleteMessage _clone = (GridGgfsDeleteMessage)_msg;

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
                if (!commState.putGridUuid(id))
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
                IgniteUuid id0 = commState.getGridUuid();

                if (id0 == GRID_UUID_NOT_READ)
                    return false;

                id = id0;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 68;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridGgfsDeleteMessage.class, this);
    }
}
