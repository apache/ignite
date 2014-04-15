/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.ggfs;

import org.gridgain.grid.*;

/**
 * Represents exception occurred during IPC endpoint binding.
 */
public class GridGgfsIpcEndpointBindException extends GridException {
    private static final long serialVersionUID = 6874137797320464838L;

    /**
     * Constructor.
     *
     * @param msg Message.
     */
    public GridGgfsIpcEndpointBindException(String msg) {
        super(msg);
    }

    /**
     * Constructor.
     *
     * @param msg Message.
     * @param cause Cause.
     */
    public GridGgfsIpcEndpointBindException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
