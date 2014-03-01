/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
package org.gridgain.client;

/**
 * This exception is thrown whenever an attempt is made to use a closed client.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridClientClosedException extends GridClientException {
    /**
     * Creates exception with given message.
     *
     * @param msg Error message.
     */
    public GridClientClosedException(String msg) {
        super(msg);
    }
}
