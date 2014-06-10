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
 * Exception indicating that field name provided during deserialization of portable object doesn't exist.
 * <p>
 * Thrown from {@link GridPortableObject#field(String)} method.
 */
public class GridPortableInvalidFieldException extends GridPortableException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Creates invalid field exception with error message.
     *
     * @param msg Error message.
     */
    public GridPortableInvalidFieldException(String msg) {
        super(msg);
    }

    /**
     * Creates invalid field exception with {@link Throwable} as a cause.
     *
     * @param cause Cause.
     */
    public GridPortableInvalidFieldException(Throwable cause) {
        super(cause);
    }

    /**
     * Creates invalid field exception with error message and {@link Throwable} as a cause.
     *
     * @param msg Error message.
     * @param cause Cause.
     */
    public GridPortableInvalidFieldException(String msg, @Nullable Throwable cause) {
        super(msg, cause);
    }
}
