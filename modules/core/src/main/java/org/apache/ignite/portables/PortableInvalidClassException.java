/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.portables;

import org.jetbrains.annotations.*;

/**
 * Exception indicating that class needed for deserialization of portable object does not exist.
 * <p>
 * Thrown from {@link PortableObject#deserialize()} method.
 */
public class PortableInvalidClassException extends PortableException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Creates invalid class exception with error message.
     *
     * @param msg Error message.
     */
    public PortableInvalidClassException(String msg) {
        super(msg);
    }

    /**
     * Creates invalid class exception with {@link Throwable} as a cause.
     *
     * @param cause Cause.
     */
    public PortableInvalidClassException(Throwable cause) {
        super(cause);
    }

    /**
     * Creates invalid class exception with error message and {@link Throwable} as a cause.
     *
     * @param msg Error message.
     * @param cause Cause.
     */
    public PortableInvalidClassException(String msg, @Nullable Throwable cause) {
        super(msg, cause);
    }
}
