/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.util;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Synchronized map for cache values that is safe to update in-place. Main reason for this map
 * is to provide snapshot-guarantee for serialization and keep concurrent iterators.
 */
public class GridSynchronizedMap<K, V> extends ConcurrentHashMap<K, V> implements Externalizable {
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