/* @java.file.header */

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

/**
 *
 */
public class GridDrExternalBatchResponse {
    /** Request identifier. */
    private GridUuid reqId;

    /** Error message. */
    private String errMsg;

    /**
     * @param reqId Batch ID.
     * @param errMsg Error message.
     */
    public GridDrExternalBatchResponse(GridUuid reqId, @Nullable String errMsg) {
        assert reqId != null;

        this.reqId = reqId;
        this.errMsg = errMsg;
    }

    /**
     * @return Batch ID.
     */
    public GridUuid requestId() {
        return reqId;
    }

    /**
     * @return Error message.
     */
    @Nullable public String errorMessage() {
        return errMsg;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDrExternalBatchResponse.class, this);
    }
}
