/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.portables;

import org.gridgain.grid.*;
import org.jetbrains.annotations.*;

/**
 * Exception indicating portable object serialization error.
 */
public class PortableException extends GridRuntimeException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Creates portable exception with error message.
     *
     * @param msg Error message.
     */
    public PortableException(String msg) {
        super(msg);
    }

    /**
     * Creates portable exception with {@link Throwable} as a cause.
     *
     * @param cause Cause.
     */
    public PortableException(Throwable cause) {
        super(cause);
    }

    /**
     * Creates portable exception with error message and {@link Throwable} as a cause.
     *
     * @param msg Error message.
     * @param cause Cause.
     */
    public PortableException(String msg, @Nullable Throwable cause) {
        super(msg, cause);
    }
}
