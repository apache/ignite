/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.compute;

import org.gridgain.grid.*;
import org.jetbrains.annotations.*;

/**
 * This exception is thrown when user's code throws undeclared runtime exception. By user core it is
 * assumed the code in grid task, grid job or SPI. In most cases it should be an indication of unrecoverable
 * error condition such as assertion, {@link NullPointerException}, {@link OutOfMemoryError}, etc.
 */
public class ComputeUserUndeclaredException extends GridException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Creates user undeclared exception with given task execution ID and
     * error message.
     *
     * @param msg Error message.
     */
    public ComputeUserUndeclaredException(String msg) {
        super(msg);
    }

    /**
     * Creates new user undeclared exception given throwable as a cause and
     * source of error message.
     *
     * @param cause Non-null throwable cause.
     */
    public ComputeUserUndeclaredException(Throwable cause) {
        this(cause.getMessage(), cause);
    }

    /**
     * Creates user undeclared exception with given task execution ID,
     * error message and optional nested exception.
     *
     * @param msg Error message.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public ComputeUserUndeclaredException(String msg, @Nullable Throwable cause) {
        super(msg, cause);
    }
}
