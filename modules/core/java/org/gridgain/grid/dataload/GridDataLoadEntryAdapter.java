// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.dataload;

import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;

/**
 * Convenience adapter for {@link GridDataLoadEntry}.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridDataLoadEntryAdapter<K, V> implements GridDataLoadEntry<K, V>, Externalizable {
    /** */
    private K key;

    /** */
    private V val;

    /**
     * For {@link Externalizable}.
     */
    public GridDataLoadEntryAdapter() {
        // No-op.
    }

    /**
     * Creates new entry.
     *
     * @param key Key.
     * @param val Value or {@code null} if it is a 'remove' entry.
     */
    public GridDataLoadEntryAdapter(K key, @Nullable V val) {
        A.notNull(key, "key");

        this.key = key;
        this.val = val;
    }

    /** {@inheritDoc} */
    @Override public K key() throws GridException {
        return key;
    }

    /** {@inheritDoc} */
    @Nullable @Override public V value() throws GridException {
        return val;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(key);
        out.writeObject(val);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        key = (K)in.readObject();
        val = (V)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDataLoadEntryAdapter.class, this);
    }
}
