/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.dr.messages.external;

import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

/**
 * Response which is sent by receiving hub as result of {@link GridDrExternalHandshakeRequest} processing.
 */
public class GridDrExternalHandshakeResponse {
    /** Error message. */
    private String errMsg;

    /**
     * Constructor.
     *
     * @param errMsg Error message.
     */
    public GridDrExternalHandshakeResponse(@Nullable String errMsg) {
        this.errMsg = errMsg;
    }

    /**
     * @return Error message.
     */
    @Nullable public String errorMessage() {
        return errMsg;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDrExternalHandshakeResponse.class, this);
    }
}
