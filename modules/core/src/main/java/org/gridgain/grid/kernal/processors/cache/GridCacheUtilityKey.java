/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import java.io.*;

/**
 * Key for system utility cache.
 */
public abstract class GridCacheUtilityKey<K extends GridCacheUtilityKey> implements GridCacheInternal, Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked") @Override
    public final boolean equals(Object obj) {
        return obj == this || obj != null && obj.getClass() == getClass() && equalsx((K)obj);
    }

    /**
     * Child-specific equals method.
     *
     * @param key Key.
     * @return {@code True} if equals.
     */
    protected abstract boolean equalsx(K key);

    /** {@inheritDoc} */
    public abstract int hashCode();
}
