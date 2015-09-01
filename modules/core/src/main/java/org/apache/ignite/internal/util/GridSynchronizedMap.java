/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.util;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;
import org.jsr166.ConcurrentHashMap8;

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