/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.ggfs.common;

import static org.gridgain.grid.kernal.ggfs.common.GridGgfsIpcCommand.*;

/**
 * GGFS status (total/used/free space) request.
 */
public class GridGgfsStatusRequest extends GridGgfsMessage {
    /** {@inheritDoc} */
    @Override public GridGgfsIpcCommand command() {
        return STATUS;
    }

    /** {@inheritDoc} */
    @Override public void command(GridGgfsIpcCommand cmd) {
        // No-op.
    }
}
