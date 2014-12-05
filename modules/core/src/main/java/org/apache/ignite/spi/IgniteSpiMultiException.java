/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi;

import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Grid SPI exception which may contain more than one failure.
 */
public class IgniteSpiMultiException extends IgniteSpiException {
    /** */
    private static final long serialVersionUID = 0L;

    /** Nested exceptions. */
    private List<Throwable> causes = new ArrayList<>();

    /**
     * Creates new exception with given error message.
     *
     * @param msg Error message.
     */
    public IgniteSpiMultiException(String msg) {
        super(msg);
    }

    /**
     * Creates new grid exception with given throwable as a cause and
     * source of error message.
     *
     * @param cause Non-null throwable cause.
     */
    public IgniteSpiMultiException(Throwable cause) {
        this(cause.getMessage(), cause);
    }

    /**
     * Creates new exception with given error message and optional nested exception.
     *
     * @param msg Error message.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public IgniteSpiMultiException(String msg, @Nullable Throwable cause) {
        super(msg, cause);
    }

    /**
     * Creates new exception with given error message and optional nested exception.
     *
     * @param msg Error message.
     * @param cause Optional nested exception (can be {@code null}).
     * @param nestedCauses Optional collection of nested causes.
     */
    public IgniteSpiMultiException(String msg, @Nullable Throwable cause, @Nullable Collection<Throwable> nestedCauses) {
        super(msg, cause);

        if (nestedCauses != null)
            causes.addAll(nestedCauses);
    }

    /**
     * Adds a new cause for multi-exception.
     *
     * @param cause Cause to add.
     */
    public void add(Throwable cause) {
        causes.add(cause);
    }

    /**
     * Gets nested causes for this multi-exception.
     *
     * @return Nested causes for this multi-exception.
     */
    public List<Throwable> nestedCauses() {
        return causes;
    }

    /** {@inheritDoc} */
    @Override public void printStackTrace(PrintStream s) {
        super.printStackTrace(s);

        for (Throwable t : causes)
            t.printStackTrace(s);
    }
}
