/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
package org.gridgain.client;

/**
 * Thrown when none of the servers from configuration can be connected within a timeout.
 */
public class GridServerUnreachableException extends GridClientException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Creates exception with specified error message.
     *
     * @param msg Error message.
     */
    public GridServerUnreachableException(String msg) {
        super(msg);
    }

    /**
     * Creates exception with specified error message and cause.
     *
     * @param msg Error message.
     * @param cause Error cause.
     */
    public GridServerUnreachableException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
