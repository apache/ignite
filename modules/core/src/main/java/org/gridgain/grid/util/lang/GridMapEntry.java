/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.lang;

import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 * Simple {@link Map.Entry} implementation.
 */
public class GridMapEntry<K, V> implements Map.Entry<K, V>, Serializable {
    /** */
    @GridToStringInclude
    private K key;

    @GridToStringInclude
    /** */
    private V val;

    /**
     * @param key Key.
     * @param val Value.
     */
    public GridMapEntry(K key, V val) {
        this.key = key;
        this.val = val;
    }

    /** {@inheritDoc} */
    @Override public K getKey() {
        return key;
    }

    /** {@inheritDoc} */
    @Override public V getValue() {
        return val;
    }

    /** {@inheritDoc} */
    @Override public V setValue(V val) {
        V old = this.val;

        this.val = val;

        return old;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        GridMapEntry e = (GridMapEntry)o;

        return F.eq(key, e.key) && F.eq(val, e.val);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return 31 * (key != null ? key.hashCode() : 0) + (val != null ? val.hashCode() : 0);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridMapEntry.class, this);
    }
}
