/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util;

import org.jdk8.backport.*;

import java.io.*;
import java.util.*;

/**
 * Synchronized map for cache values that is safe to update in-place. Main reason for this map
 * is to provide snapshot-guarantee for serialization and keep concurrent iterators.
 */
public class GridSynchronizedMap<K, V> extends ConcurrentHashMap8<K, V> implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override public synchronized V putIfAbsent(K key, V val) {
        return super.putIfAbsent(key, val);
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean remove(Object key, Object val) {
        return super.remove(key, val);
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean replace(K key, V oldVal, V newVal) {
        return super.replace(key, oldVal, newVal);
    }

    /** {@inheritDoc} */
    @Override public synchronized V replace(K key, V val) {
        return super.replace(key, val);
    }

    /** {@inheritDoc} */
    @Override public synchronized V put(K key, V val) {
        return super.put(key, val);
    }

    /** {@inheritDoc} */
    @Override public synchronized V remove(Object key) {
        return super.remove(key);
    }

    /** {@inheritDoc} */
    @Override public synchronized void putAll(Map<? extends K, ? extends V> m) {
        super.putAll(m);
    }

    /** {@inheritDoc} */
    @Override public synchronized void clear() {
        super.clear();
    }

    /** {@inheritDoc} */
    @Override public synchronized void writeExternal(ObjectOutput out) throws IOException {
        int size = size();

        out.writeInt(size);

        for (Entry<K, V> entry : entrySet()) {
            out.writeObject(entry.getKey());
            out.writeObject(entry.getValue());

            size--;
        }

        assert size == 0 : "Invalid number of entries written: " + size;
    }

    /** {@inheritDoc} */
    @Override public synchronized void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int size = in.readInt();

        for (int i = 0; i < size; i++)
            put((K)in.readObject(), (V)in.readObject());

        int mapSize = size();

        assert mapSize == size : "Invalid map size after reading [size=" + size + ", mapSize=" + size + ']';
    }
}
