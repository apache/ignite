/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

/**
 * Distributed future aware of MVCC locking.
 */
public interface GridCacheMvccFuture<K, V, T> extends GridCacheFuture<T> {
    /**
     * @param entry Entry which received new owner.
     * @param owner Owner.
     * @return {@code True} if future cares about this entry.
     */
    public boolean onOwnerChanged(GridCacheEntryEx<K,V> entry, GridCacheMvccCandidate<K> owner);
}
