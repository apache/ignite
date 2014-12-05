/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.ggfs;

import org.gridgain.grid.ggfs.*;

/**
 * Exception indicating that directory can not be deleted because it is not empty.
 */
public class GridGgfsDirectoryNotEmptyException extends IgniteFsException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * @param msg Exception message.
     */
    public GridGgfsDirectoryNotEmptyException(String msg) {
        super(msg);
    }

    /**
     * Creates an instance of GGFS exception caused by nested exception.
     *
     * @param cause Exception cause.
     */
    public GridGgfsDirectoryNotEmptyException(Throwable cause) {
        super(cause);
    }
}
