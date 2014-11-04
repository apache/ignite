/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import java.util.*;

/**
 * Local transaction API.
 */
public interface GridCacheTxRemoteEx<K, V> extends GridCacheTxEx<K, V> {
    /**
     * @return Remote thread ID.
     */
    public long remoteThreadId();

    /**
     * @param baseVer Base version.
     * @param committedVers Committed version.
     * @param rolledbackVers Rolled back version.
     * @param pendingVers Pending versions.
     */
    public void doneRemote(GridCacheVersion baseVer, Collection<GridCacheVersion> committedVers,
        Collection<GridCacheVersion> rolledbackVers, Collection<GridCacheVersion> pendingVers);

    /**
     * @param e Sets write value for pessimistic transactions.
     * @return {@code True} if entry was found.
     */
    public boolean setWriteValue(GridCacheTxEntry<K, V> e);
}
