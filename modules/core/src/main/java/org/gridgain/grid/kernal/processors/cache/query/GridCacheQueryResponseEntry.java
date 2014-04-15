/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.query;

import org.gridgain.grid.util.tostring.*;

import java.io.*;
import java.util.*;

/**
 * Class to store query results returned by remote nodes. It's required to fully
 * control serialization process. Local entries can be returned to user as is.
 */
public class GridCacheQueryResponseEntry<K, V> implements Map.Entry<K, V>, Externalizable {
    private static final long serialVersionUID = 0L;

    /** */
    @GridToStringInclude
    private K key;

    /** */
    @GridToStringInclude
    private V val;

    /**
     * Required by {@link Externalizable}.
     */
    public GridCacheQueryResponseEntry() {
        // No-op.
    }

    /**
     * @param key Key.
     * @param val Value.
     */
    public GridCacheQueryResponseEntry(K key, V val) {
        this.key = key;
        this.val = val;
    }

    /** @return Key. */
    @Override public K getKey() {
        return key;
    }

    /**
     * @return Value.
     */
    @Override public V getValue() {
        return val;
    }

    /**
     * @param val Value
     */
    @Override public V setValue(V val) {
        this.val = val;

        return val;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(key);
        out.writeObject(val);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        key = (K)in.readObject();
        val = (V)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        GridCacheQueryResponseEntry entry = (GridCacheQueryResponseEntry)o;

        return key.equals(entry.key);

    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return key.hashCode();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "[" + key + "=" + val + "]";
    }
}
