/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.ggfs;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Fragmentizer request. Sent from coordinator to other GGFS nodes when colocated part of file
 * should be fragmented.
 */
public class GridGgfsFragmentizerRequest extends GridGgfsCommunicationMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** File id. */
    private GridUuid fileId;

    /** Ranges to fragment. */
    @GridToStringInclude
    @GridDirectCollection(GridGgfsFileAffinityRange.class)
    private Collection<GridGgfsFileAffinityRange> fragmentRanges;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridGgfsFragmentizerRequest() {
        // No-op.
    }

    /**
     * @param fileId File id to fragment.
     * @param fragmentRanges Ranges to fragment.
     */
    public GridGgfsFragmentizerRequest(GridUuid fileId, Collection<GridGgfsFileAffinityRange> fragmentRanges) {
        this.fileId = fileId;
        this.fragmentRanges = fragmentRanges;
    }

    /**
     * @return File ID.
     */
    public GridUuid fileId() {
        return fileId;
    }

    /**
     * @return Fragment ranges.
     */
    public Collection<GridGgfsFileAffinityRange> fragmentRanges() {
        return fragmentRanges;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridGgfsFragmentizerRequest.class, this);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridGgfsFragmentizerRequest _clone = new GridGgfsFragmentizerRequest();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        super.clone0(_msg);

        GridGgfsFragmentizerRequest _clone = (GridGgfsFragmentizerRequest)_msg;

        _clone.fileId = fileId;
        _clone.fragmentRanges = fragmentRanges;
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
                if (!commState.putGridUuid(fileId))
                    return false;

                commState.idx++;

            case 1:
                if (fragmentRanges != null) {
                    if (commState.it == null) {
                        if (!commState.putInt(fragmentRanges.size()))
                            return false;

                        commState.it = fragmentRanges.iterator();
                    }

                    while (commState.it.hasNext() || commState.cur != NULL) {
                        if (commState.cur == NULL)
                            commState.cur = commState.it.next();

                        if (!commState.putMessage((GridGgfsFileAffinityRange)commState.cur))
                            return false;

                        commState.cur = NULL;
                    }

                    commState.it = null;
                } else {
                    if (!commState.putInt(-1))
                        return false;
                }

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
                GridUuid fileId0 = commState.getGridUuid();

                if (fileId0 == GRID_UUID_NOT_READ)
                    return false;

                fileId = fileId0;

                commState.idx++;

            case 1:
                if (commState.readSize == -1) {
                    if (buf.remaining() < 4)
                        return false;

                    commState.readSize = commState.getInt();
                }

                if (commState.readSize >= 0) {
                    if (fragmentRanges == null)
                        fragmentRanges = new ArrayList<>(commState.readSize);

                    for (int i = commState.readItems; i < commState.readSize; i++) {
                        Object _val = commState.getMessage();

                        if (_val == MSG_NOT_READ)
                            return false;

                        fragmentRanges.add((GridGgfsFileAffinityRange)_val);

                        commState.readItems++;
                    }
                }

                commState.readSize = -1;
                commState.readItems = 0;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 70;
    }
}
