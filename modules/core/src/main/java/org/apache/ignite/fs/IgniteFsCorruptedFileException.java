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
 * Exception thrown when target file's block is not found in data cache.
 */
public class IgniteFsCorruptedFileException extends IgniteFsException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * @param msg Error message.
     */
    public IgniteFsCorruptedFileException(String msg) {
        super(msg);
    }

    /**
     * @param cause Error cause.
     */
    public IgniteFsCorruptedFileException(Throwable cause) {
        super(cause);
    }

    /**
     * @param msg Error message.
     * @param cause Error cause.
     */
    public IgniteFsCorruptedFileException(String msg, @Nullable Throwable cause) {
        super(msg, cause);
    }
}
