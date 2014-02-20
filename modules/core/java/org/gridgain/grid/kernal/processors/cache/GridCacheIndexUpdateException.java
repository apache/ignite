// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.*;
import org.jetbrains.annotations.*;

/**
 * Exception indicating that index update failed during cache entry update. If this exception is thrown, entry
 * is kept in it's original state (no updates performed).
 *
 * @author @java.author
 * @version @java.version
 */
public class GridCacheIndexUpdateException extends GridException {
    /**
     * @param cause Exception cause.
     */
    public GridCacheIndexUpdateException(Throwable cause) {
        super(cause);
    }

    /**
     * @param msg Error message.
     * @param cause Error cause.
     */
    public GridCacheIndexUpdateException(String msg, @Nullable Throwable cause) {
        super(msg, cause);
    }
}
