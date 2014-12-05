/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi;

import org.gridgain.grid.*;

/**
 * Exception thrown by SPI implementations.
 */
public class IgniteSpiException extends GridException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Creates new SPI exception with given error message.
     *
     * @param msg Error message.
     */
    public IgniteSpiException(String msg) {
        super(msg);
    }

    /**
     * Creates new SPI exception given throwable as a cause and
     * source of error message.
     *
     * @param cause Non-null throwable cause.
     */
    public IgniteSpiException(Throwable cause) {
        this(cause.getMessage(), cause);
    }

    /**
     * Creates new SPI exception with given error message and optional nested exception.
     *
     * @param msg Error message.
     * @param cause Optional nested message.
     */
    public IgniteSpiException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
