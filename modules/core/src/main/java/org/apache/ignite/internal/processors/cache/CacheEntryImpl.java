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

package org.apache.ignite.internal.processors.cache;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import javax.cache.Cache;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

/**
 *
 */
public class CacheEntryImpl<K, V> implements Cache.Entry<K, V>, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private K key;

    /** */
    private V val;

    /** Entry version. */
    private GridCacheVersion ver;

    /**
     * Required by {@link Externalizable}.
     */
    public CacheEntryImpl() {
        // No-op.
    }

    /**
     * @param key Key.
     * @param val Value.
     */
    public CacheEntryImpl(K key, V val) {
        this.key = key;
        this.val = val;
    }

    /**
     * @param key Key.
     * @param val Value.
     * @param ver Entry version.
     */
    public CacheEntryImpl(K key, V val, GridCacheVersion ver) {
        this.key = key;
        this.val = val;
        this.ver = ver;
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
    @SuppressWarnings("unchecked")
    @Override public <T> T unwrap(Class<T> cls) {
        if (cls.isAssignableFrom(getClass()))
            return cls.cast(this);

        if (cls.isAssignableFrom(CacheEntry.class))
            return (T)new CacheEntryImplEx<>(key, val, ver);

        throw new IllegalArgumentException("Unwrapping to class is not supported: " + cls);
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
        return "Entry [key=" + key + ", val=" + val + ']';
    }
}
