// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client;

/**
 * Client exception is a common super class of all client exceptions.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridClientException extends Exception {
    /**
     * Constructs client exception.
     *
     * @param msg Message.
     */
    public GridClientException(String msg) {
        super(msg);
    }

    /**
     * Constructs client exception.
     *
     * @param msg Message.
     * @param cause Cause.
     */
    public GridClientException(String msg, Throwable cause) {
        super(msg, cause);
    }

    /**
     * Constructs client exception.
     *
     * @param cause Cause.
     */
    public GridClientException(Throwable cause) {
        super(cause);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return getClass() + ": " + getMessage();
    }
}
