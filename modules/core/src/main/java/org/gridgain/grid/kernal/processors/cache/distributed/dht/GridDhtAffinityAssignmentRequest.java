/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.dht;

import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.nio.*;

/**
 * Affinity assignment request.
 */
public class GridDhtAffinityAssignmentRequest<K, V> extends GridCacheMessage<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Topology version being queried. */
    private long topVer;

    /**
     * Empty constructor.
     */
    public GridDhtAffinityAssignmentRequest() {
        // No-op.
    }

    /**
     * @param cacheId Cache ID.
     * @param topVer Topology version.
     */
    public GridDhtAffinityAssignmentRequest(int cacheId, long topVer) {
        this.cacheId = cacheId;
        this.topVer = topVer;
    }

    /** {@inheritDoc} */
    @Override public boolean allowForStartup() {
        return true;
    }

    /**
     * @return Requested topology version.
     */
    @Override public long topologyVersion() {
        return topVer;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 79;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridDhtAffinityAssignmentRequest _clone = new GridDhtAffinityAssignmentRequest();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        super.clone0(_msg);

        GridDhtAffinityAssignmentRequest _clone = (GridDhtAffinityAssignmentRequest)_msg;

        _clone.topVer = topVer;
    }

    /** {@inheritDoc} */
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
            case 3:
                if (!commState.putLong("topVer", topVer))
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
            case 3:
                topVer = commState.getLong("topVer");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtAffinityAssignmentRequest.class, this);
    }
}
