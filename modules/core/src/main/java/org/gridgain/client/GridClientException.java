/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client;

import org.gridgain.grid.util.typedef.*;
import org.jetbrains.annotations.*;

/**
 * Client exception is a common super class of all client exceptions.
 */
public class GridClientException extends Exception {
    /** */
    private static final long serialVersionUID = 0L;

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

    /**
     * Checks if passed in {@code 'Throwable'} has given class in {@code 'cause'} hierarchy
     * <b>including</b> that throwable itself.
     * <p>
     * Note that this method follows includes {@link Throwable#getSuppressed()}
     * into check.
     *
     * @param cls Cause classes to check (if {@code null} or empty, {@code false} is returned).
     * @return {@code True} if one of the causing exception is an instance of passed in classes,
     *      {@code false} otherwise.
     */
    public boolean hasCause(@Nullable Class<? extends Throwable>... cls) {
        return hasCause(this, cls);
    }

    /**
     * Checks if passed in {@code 'Throwable'} has given class in {@code 'cause'} hierarchy
     * <b>including</b> that throwable itself.
     * <p>
     * Note that this method follows includes {@link Throwable#getSuppressed()}
     * into check.
     *
     * @param t Throwable to check (if {@code null}, {@code false} is returned).
     * @param cls Cause classes to check (if {@code null} or empty, {@code false} is returned).
     * @return {@code True} if one of the causing exception is an instance of passed in classes,
     *      {@code false} otherwise.
     */
    private boolean hasCause(@Nullable Throwable t, @Nullable Class<? extends Throwable>... cls) {
        if (t == null || F.isEmpty(cls))
            return false;

        assert cls != null;

        for (Throwable th = t; th != null; th = th.getCause()) {
            for (Class<? extends Throwable> c : cls)
                if (c.isAssignableFrom(th.getClass()))
                    return true;

            for (Throwable n : th.getSuppressed())
                if (hasCause(n, cls))
                    return true;

            if (th.getCause() == th)
                break;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return getClass() + ": " + getMessage();
    }
}
