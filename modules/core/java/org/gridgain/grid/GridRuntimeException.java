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
 * Common runtime exception for grid. Thrown by all components wherever
 * runtime exception is needed.
 */
public class GridRuntimeException extends RuntimeException {
    private static final long serialVersionUID = 6240457084393651371L;

    /**
     * Constructs runtime grid exception with given message and cause.
     *
     * @param msg Exception message.
     * @param cause Exception cause.
     */
    public GridRuntimeException(String msg, @Nullable Throwable cause) {
        super(msg, cause);
    }

    /**
     * Creates new runtime grid exception given throwable as a cause and
     * source of error message.
     *
     * @param cause Non-null throwable cause.
     */
    public GridRuntimeException(Throwable cause) {
        this(cause.getMessage(), cause);
    }

    /**
     * Constructs runtime grid exception with given message.
     *
     * @param msg Exception message.
     */
    public GridRuntimeException(String msg) {
        super(msg);
    }

    /**
     * Checks if this exception has given class in {@code 'cause'} hierarchy.
     *
     * @param cls Cause class to check (if {@code null}, {@code false} is returned)..
     * @return {@code True} if one of the causing exception is an instance of passed in class,
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

    /** {@inheritDoc} */
    @Override public String getMessage() {
        return X.hasCauseExcludeRoot(this, GridException.class, GridRuntimeException.class) ?
            super.getMessage() : errorMessageWithHelpUrls(super.getMessage());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return getClass() + ": " + getMessage();
    }
}
