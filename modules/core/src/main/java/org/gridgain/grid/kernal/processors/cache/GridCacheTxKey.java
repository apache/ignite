/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

/**
 * Cache transaction key. This wrapper is needed because same keys may be enlisted in the same transaction
 * for multiple caches.
 */
public class GridCacheTxKey<K> implements Externalizable {
    /** Key. */
    @GridToStringInclude
    private K key;

    /** Cache ID. */
    private int cacheId;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridCacheTxKey() {
        // No-op.
    }

    /**
     * @param key User key.
     * @param cacheId Cache ID.
     */
    public GridCacheTxKey(K key, int cacheId) {
        this.key = key;
        this.cacheId = cacheId;
    }

    /**
     * @return User key.
     */
    public K key() {
        return key;
    }

    /**
     * @return Cache ID.
     */
    public int cacheId() {
        return cacheId;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof GridCacheTxKey))
            return false;

        GridCacheTxKey that = (GridCacheTxKey)o;

        return cacheId == that.cacheId && key.equals(that.key);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = key.hashCode();

        res = 31 * res + cacheId;

        return res;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(cacheId);
        out.writeObject(key);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        cacheId = in.readInt();
        key = (K)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheTxKey.class, this);
    }
}
