/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.datastructures;

import org.apache.ignite.*;
import org.jetbrains.annotations.*;

/**
 * This checked exception gets thrown if attempt to access a removed data structure has been made.
 * <p>
 * Note that data structures throw runtime exceptions out of methods that don't have
 * checked exceptions in the signature.
 */
public class GridCacheDataStructureRemovedException extends IgniteCheckedException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Creates new exception with given error message.
     *
     * @param msg Error message.
     */
    public GridCacheDataStructureRemovedException(String msg) {
        super(msg);
    }

    /**
     * Creates new exception with given throwable as a nested cause and
     * source of error message.
     *
     * @param cause Non-null throwable cause.
     */
    public GridCacheDataStructureRemovedException(Throwable cause) {
        this(cause.getMessage(), cause);
    }

    /**
     * Creates a new exception with given error message and optional nested cause exception.
     *
     * @param msg Error message.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public GridCacheDataStructureRemovedException(String msg, @Nullable Throwable cause) {
        super(msg, cause);
    }
}
