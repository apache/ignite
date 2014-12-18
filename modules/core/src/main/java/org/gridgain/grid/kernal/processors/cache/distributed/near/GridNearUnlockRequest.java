/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.near;

import org.gridgain.grid.kernal.processors.cache.distributed.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.nio.*;

/**
 * Near cache unlock request.
 */
public class GridNearUnlockRequest<K, V> extends GridDistributedUnlockRequest<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridNearUnlockRequest() {
        // No-op.
    }

    /**
     * @param cacheId Cache ID.
     * @param keyCnt Key count.
     */
    public GridNearUnlockRequest(int cacheId, int keyCnt) {
        super(cacheId, keyCnt);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridNearUnlockRequest _clone = new GridNearUnlockRequest();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        super.clone0(_msg);

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

        return true;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean readFrom(ByteBuffer buf) {
        commState.setBuffer(buf);

        if (!super.readFrom(buf))
            return false;

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 56;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearUnlockRequest.class, this, super.toString());
    }
}
