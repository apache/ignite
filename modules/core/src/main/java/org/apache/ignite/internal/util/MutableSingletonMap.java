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

import java.util.AbstractMap;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Map that contains only one key/value. By default the map contains null/null entry.
 */
public class MutableSingletonMap<K, V> extends AbstractMap<K, V> {
    /** The key of single entry. */
    protected K key;

    /** The value of single entry. */
    protected V value;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public Set<Entry<K, V>> entrySet() {
        return (Set)Collections.singleton(new SimpleEntry<>(key, value));
    }

    /** {@inheritDoc} */
    @Override public V put(K key, V value) {
        this.key = key;
        this.value = value;

        return value;
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        throw new UnsupportedOperationException();
    }

    /**
     * Creates filled singleton map.
     *
     * @return singleton map
     */
    @SuppressWarnings("unchecked")
    public Map<K, V> singletonMap() {
        return Collections.singletonMap(key, value);
    }
}
