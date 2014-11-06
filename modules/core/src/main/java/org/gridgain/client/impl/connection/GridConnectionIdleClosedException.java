/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
package org.gridgain.client.impl.connection;

/**
 * This exception is thrown if client was closed by idle checker thread. This exception should be
 * handled internally and never rethrown to user.
 */
public class GridConnectionIdleClosedException extends GridClientConnectionResetException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Creates exception with error message.
     *
     * @param msg Error message.
     */
    GridConnectionIdleClosedException(String msg) {
        super(msg);
    }
}
