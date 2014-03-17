// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.dr.cache.sender;

import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;

/**
 * Data center replication pause value object.
 */
public class GridDrPause implements Externalizable {
    /** Reason of DR pause {@code null} if not paused. */
    private GridDrPauseReason reason;

    /** Optional error information. */
    private String errMsg;

    /**
     * Required by {@link Externalizable}.
     */
    public GridDrPause() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param reason Reason of replication pause.
     * @param errMsg Optional error information.
     */
    public GridDrPause(GridDrPauseReason reason, @Nullable String errMsg) {
        this.reason = reason;
        this.errMsg = errMsg;
    }

    /**
     * @return Reason of replication pause.
     */
    public GridDrPauseReason reason() {
        return reason;
    }

    /**
     * @return Error information.
     */
    @Nullable public String error() {
        return errMsg;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeEnum(out, reason);
        U.writeString(out, errMsg);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        reason = U.readEnum(in, GridDrPauseReason.class);
        errMsg = U.readString(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return super.toString();
    }
}
