// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.dr.messages.external;

import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;

/**
 * @author @java.author
 * @version @java.version
 */
public class GridDrExternalBatchResponse implements Externalizable {
    /** Request identifier. */
    private GridUuid reqId;

    /** Exception which occurred during request processing. */
    private Throwable err;

    /**
     * @param reqId Batch ID.
     * @param err Error occurred during request processing.
     */
    public GridDrExternalBatchResponse(GridUuid reqId, @Nullable Throwable err) {
        assert reqId != null;

        this.reqId = reqId;
        this.err = err;
    }

    /**
     * {@link Externalizable} support.
     */
    public GridDrExternalBatchResponse() {
        // No-op.
    }

    /**
     * @return Batch ID.
     */
    public GridUuid requestId() {
        return reqId;
    }

    /**
     * @return Error occurred during request processing.
     */
    @Nullable public Throwable error() {
        return err;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeGridUuid(out, reqId);
        out.writeObject(err);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        reqId = U.readGridUuid(in);
        err = (Throwable)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDrExternalBatchResponse.class, this);
    }
}
