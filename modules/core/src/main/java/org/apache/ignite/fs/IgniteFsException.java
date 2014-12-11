/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.fs;

import org.apache.ignite.*;
import org.jetbrains.annotations.*;

/**
 * {@code GGFS} exception thrown by file system components.
 */
public class IgniteFsException extends IgniteCheckedException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Creates an instance of GGFS exception with descriptive error message.
     *
     * @param msg Error message.
     */
    public IgniteFsException(String msg) {
        super(msg);
    }

    /**
     * Creates an instance of GGFS exception caused by nested exception.
     *
     * @param cause Exception cause.
     */
    public IgniteFsException(Throwable cause) {
        super(cause);
    }

    /**
     * Creates an instance of GGFS exception with error message and underlying cause.
     *
     * @param msg Error message.
     * @param cause Exception cause.
     */
    public IgniteFsException(String msg, @Nullable Throwable cause) {
        super(msg, cause);
    }
}
