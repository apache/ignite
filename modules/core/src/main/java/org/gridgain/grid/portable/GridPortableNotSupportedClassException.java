/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.portable;

import org.jetbrains.annotations.*;

/**
 * Exception indicating that class that doesn't implement {@link GridPortable} is being serialized.
 */
public class GridPortableNotSupportedClassException extends GridPortableException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Creates invalid class exception with error message.
     *
     * @param msg Error message.
     */
    public GridPortableNotSupportedClassException(String msg) {
        super(msg);
    }

    /**
     * Creates invalid class exception with {@link Throwable} as a cause.
     *
     * @param cause Cause.
     */
    public GridPortableNotSupportedClassException(Throwable cause) {
        super(cause);
    }

    /**
     * Creates invalid class exception with error message and {@link Throwable} as a cause.
     *
     * @param msg Error message.
     * @param cause Cause.
     */
    public GridPortableNotSupportedClassException(String msg, @Nullable Throwable cause) {
        super(msg, cause);
    }
}
