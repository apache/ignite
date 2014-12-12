/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid;

import org.apache.ignite.*;

/**
 * Exception that represents authentication failure.
 */
public class GridAuthenticationException extends IgniteCheckedException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Creates authentication exception with given error message.
     *
     * @param msg Error message.
     */
    public GridAuthenticationException(String msg) {
        super(msg);
    }
}
