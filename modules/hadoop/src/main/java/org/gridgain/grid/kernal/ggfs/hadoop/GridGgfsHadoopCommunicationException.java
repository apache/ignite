/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.ggfs.hadoop;

import org.gridgain.grid.*;

import java.io.*;

/**
 * Communication exception indicating a problem between file system and GGFS instance.
 */
public class GridGgfsHadoopCommunicationException extends GridException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Creates new exception with given throwable as a nested cause and
     * source of error message.
     *
     * @param cause Non-null throwable cause.
     */
    public GridGgfsHadoopCommunicationException(Exception cause) {
        super(cause);
    }

    /**
     * Creates a new exception with given error message and optional nested cause exception.
     *
     * @param msg Error message.
     */
    public GridGgfsHadoopCommunicationException(String msg) {
        super(msg);
    }

    /**
     * Creates a new exception with given error message and optional nested cause exception.
     *
     * @param msg Error message.
     * @param cause Cause.
     */
    public GridGgfsHadoopCommunicationException(String msg, Exception cause) {
        super(msg, cause);
    }
}
