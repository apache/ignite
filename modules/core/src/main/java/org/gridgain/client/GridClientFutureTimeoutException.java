/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
package org.gridgain.client;

/**
 * Client future timeout exception is thrown whenever any client waiting is timed out.
 */
public class GridClientFutureTimeoutException extends GridClientException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Creates exception with specified error message.
     *
     * @param msg Error message.
     */
    public GridClientFutureTimeoutException(String msg) {
        super(msg);
    }

    /**
     * Creates exception with specified error message and cause.
     *
     * @param msg Error message.
     * @param cause Error cause.
     */
    public GridClientFutureTimeoutException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
