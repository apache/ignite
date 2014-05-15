/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.gridify;

/**
 * Test gridify exception.
 */
public class GridTestGridifyException extends Exception {
    /**
     * @param msg Message.
     */
    public GridTestGridifyException(String msg) {
        super(msg);
    }

    /**
     * @param msg Message.
     * @param cause Exception cause.
     */
    public GridTestGridifyException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
