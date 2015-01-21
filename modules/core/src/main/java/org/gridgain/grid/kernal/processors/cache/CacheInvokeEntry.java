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

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.internal.*;

import javax.cache.processor.*;

/**
 * Implementation of {@link MutableEntry} passed to the {@link EntryProcessor#process(MutableEntry, Object...)}.
 */
public class CacheInvokeEntry<K, V> implements MutableEntry<K, V> {
    /** */
    @GridToStringInclude
    private final K key;

    /** */
    @GridToStringInclude
    private V val;

    /** */
    private boolean modified;

    /**
     * @param key Key.
     * @param val Value.
     */
    public CacheInvokeEntry(K key, V val) {
        this.key = key;
        this.val = val;
    }

    /** {@inheritDoc} */
    @Override public boolean exists() {
        return val != null;
    }

    /** {@inheritDoc} */
    @Override public void remove() {
        val = null;

        modified = true;
    }

    /** {@inheritDoc} */
    @Override public void setValue(V val) {
        if (val == null)
            throw new NullPointerException();

        this.val = val;

        modified = true;
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
    @Override public <T> T unwrap(Class<T> clazz) {
        throw new IllegalArgumentException();
    }

    /**
     * @return {@code True} if {@link #setValue} or {@link #remove was called}.
     */
    public boolean modified() {
        return modified;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheInvokeEntry.class, this);
    }
}
