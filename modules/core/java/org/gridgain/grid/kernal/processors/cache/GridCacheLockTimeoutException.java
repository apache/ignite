/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

/**
 * Used to internally indicate that lock acquisition has timed out.
 */
public class GridCacheLockTimeoutException extends Exception {
    /**
     * @param ver Lock version.
     */
    public GridCacheLockTimeoutException(GridCacheVersion ver) {
        super("Lock acquisition timed out: " + ver);
    }
}