/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.ggfs;

import org.apache.ignite.*;

/**
 * Internal exception thrown when attempted to update range that is no longer present
 * in file affinity map.
 */
public class GridGgfsInvalidRangeException extends IgniteCheckedException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * @param msg Error message.
     */
    public GridGgfsInvalidRangeException(String msg) {
        super(msg);
    }

    /**
     * @param cause Error cause.
     */
    public GridGgfsInvalidRangeException(Throwable cause) {
        super(cause);
    }
}
