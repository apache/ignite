/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.fs;

import org.jetbrains.annotations.*;

/**
 * {@code GGFS} exception indicating that operation target is invalid
 * (e.g. not a file while expecting to be a file).
 */
public class IgniteFsInvalidPathException extends IgniteFsException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Creates exception with given error message.
     *
     * @param msg Error message.
     */
    public IgniteFsInvalidPathException(String msg) {
        super(msg);
    }

    /**
     * Creates exception with given exception cause.
     *
     * @param cause Exception cause.
     */
    public IgniteFsInvalidPathException(Throwable cause) {
        super(cause);
    }

    /**
     * Creates exception with given error message and exception cause.
     *
     * @param msg Error message.
     * @param cause Error cause.
     */
    public IgniteFsInvalidPathException(String msg, @Nullable Throwable cause) {
        super(msg, cause);
    }
}
