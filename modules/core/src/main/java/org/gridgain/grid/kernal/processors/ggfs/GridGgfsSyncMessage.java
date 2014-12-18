/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.ggfs;

import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.nio.*;

/**
 * Basic sync message.
 */
public class GridGgfsSyncMessage extends GridGgfsCommunicationMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Coordinator node order. */
    private long order;

    /** Response flag. */
    private boolean res;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridGgfsSyncMessage() {
        // No-op.
    }

    /**
     * @param order Node order.
     * @param res Response flag.
     */
    public GridGgfsSyncMessage(long order, boolean res) {
        this.order = order;
        this.res = res;
    }

    /**
     * @return Coordinator node order.
     */
    public long order() {
        return order;
    }

    /**
     * @return {@code True} if response message.
     */
    public boolean response() {
        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridGgfsSyncMessage.class, this);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridGgfsSyncMessage _clone = new GridGgfsSyncMessage();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        super.clone0(_msg);

        GridGgfsSyncMessage _clone = (GridGgfsSyncMessage)_msg;

        _clone.order = order;
        _clone.res = res;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean writeTo(ByteBuffer buf) {
        commState.setBuffer(buf);

        if (!super.writeTo(buf))
            return false;

        if (!commState.typeWritten) {
            if (!commState.putByte(null, directType()))
                return false;

            commState.typeWritten = true;
        }

        switch (commState.idx) {
            case 0:
                if (!commState.putLong(null, order))
                    return false;

                commState.idx++;

            case 1:
                if (!commState.putBoolean(null, res))
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
                if (buf.remaining() < 8)
                    return false;

                order = commState.getLong(null);

                commState.idx++;

            case 1:
                if (buf.remaining() < 1)
                    return false;

                res = commState.getBoolean(null);

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 72;
    }
}
