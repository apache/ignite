/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid;

import org.gridgain.grid.util.typedef.*;
import org.jetbrains.annotations.*;

import static org.gridgain.grid.util.GridUtils.*;

/**
 * General grid exception. This exception is used to indicate any error condition
 * within Grid.
 */
public class GridException extends Exception {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Create empty exception.
     */
    public GridException() {
        super();
    }

    /**
     * Creates new exception with given error message.
     *
     * @param msg Error message.
     */
    public GridException(String msg) {
        super(msg);
    }

    /**
     * Creates new grid exception with given throwable as a cause and
     * source of error message.
     *
     * @param cause Non-null throwable cause.
     */
    public GridException(Throwable cause) {
        this(cause.getMessage(), cause);
    }

    /**
     * Creates new exception with given error message and optional nested exception.
     *
     * @param msg Error message.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public GridException(String msg, @Nullable Throwable cause) {
        super(msg, cause);
    }

    /**
     * Checks if this exception has given class in {@code 'cause'} hierarchy.
     *
     * @param cls Cause classes to check (if {@code null} or empty, {@code false} is returned).
     * @return {@code True} if one of the causing exception is an instance of passed in classes,
     *      {@code false} otherwise.
     */
    public boolean hasCause(@Nullable Class<? extends Throwable>... cls) {
        return X.hasCause(this, cls);
    }

    /**
     * Gets first exception of given class from {@code 'cause'} hierarchy if any.
     *
     * @param cls Cause class to get cause (if {@code null}, {@code null} is returned).
     * @return First causing exception of passed in class, {@code null} otherwise.
     */
    @Nullable public <T extends Throwable> T getCause(@Nullable Class<T> cls) {
        return X.cause(this, cls);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Adds troubleshooting links if they where not added by below in {@code cause} hierarchy.
     */
    @Override public String getMessage() {
        return X.hasCauseExcludeRoot(this, GridException.class, GridRuntimeException.class) ?
            super.getMessage() : errorMessageWithHelpUrls(super.getMessage());
    }

    /**
     * Returns exception message.
     * <p>
     * Unlike {@link #getMessage()} this method never include troubleshooting links
     * to the result string.
     *
     * @return Original message.
     */
    public String getOriginalMessage() {
        return super.getMessage();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return getClass() + ": " + getMessage();
    }
}
