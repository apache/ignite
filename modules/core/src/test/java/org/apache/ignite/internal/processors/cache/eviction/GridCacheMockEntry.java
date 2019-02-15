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

package org.apache.ignite.internal.processors.cache.eviction;

import javax.cache.Cache;
import org.apache.ignite.cache.eviction.EvictableEntry;
import org.apache.ignite.internal.util.lang.GridMetadataAwareAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Mock cache entry.
 */
public class GridCacheMockEntry<K, V> extends GridMetadataAwareAdapter implements Cache.Entry<K, V>, EvictableEntry<K, V> {
    /** */
    private static final int META_KEY = EntryKey.values().length; //+1 to maximum value (test only case)

    /** */
    @GridToStringInclude
    private K key;

    /** */
    @GridToStringInclude
    private boolean evicted;

    /**
     * Constructor.
     *
     * @param key Key.
     */
    public GridCacheMockEntry(K key) {
        this.key = key;
    }

    /** {@inheritDoc} */
    @Override public K getKey() throws IllegalStateException {
        return key;
    }

    /** {@inheritDoc} */
    @Override public V getValue() throws IllegalStateException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean evict() {
        evicted = true;

        onEvicted();

        return true;
    }

    /**
     *
     */
    private void onEvicted() {
        removeAllMeta();
    }

    /** {@inheritDoc} */
    @Override public boolean isCached() {
        return !evicted;
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return 0;
    }

    /**
     * @return Evicted or not.
     */
    public boolean isEvicted() {
        return evicted;
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> T addMeta(T val) {
        return addMeta(META_KEY, val);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> T meta() {
        return meta(META_KEY);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> T removeMeta() {
        return removeMeta(META_KEY);
    }

    /** {@inheritDoc} */
    @Override public <T> boolean removeMeta(T val) {
        return removeMeta(META_KEY, val);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> T putMetaIfAbsent(T val) {
        return putMetaIfAbsent(META_KEY, val);
    }

    /** {@inheritDoc} */
    @Override public <T> boolean replaceMeta(T curVal, T newVal) {
        return replaceMeta(META_KEY, curVal, newVal);
    }

    /** {@inheritDoc} */
    @Override public <T> T unwrap(Class<T> clazz) {
        if (clazz.isAssignableFrom(getClass()))
            return clazz.cast(this);

        throw new IllegalArgumentException();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheMockEntry.class, this);
    }
}