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

package org.pcollections;

import java.util.*;

/**
 * An immutable, persistent map from non-null keys of type K to non-null values of type V.
 *
 * @param <K>
 * @param <V>
 * @author harold
 */
public interface PMap<K, V> extends Map<K, V> {
    /**
     * @param key   non-null
     * @param value non-null
     * @return a map with the mappings of this but with key mapped to value
     */
    public PMap<K, V> plus(K key, V value);

    /**
     * @param map
     * @return this combined with map, with map's mappings used for any keys in both map and this
     */
    public PMap<K, V> plusAll(Map<? extends K, ? extends V> map);

    /**
     * @param key
     * @return a map with the mappings of this but with no value for key
     */
    public PMap<K, V> minus(Object key);

    /**
     * @param keys
     * @return a map with the mappings of this but with no value for any element of keys
     */
    public PMap<K, V> minusAll(Collection<?> keys);

    @Deprecated
    V put(K k, V v);

    @Deprecated
    V remove(Object k);

    @Deprecated
    void putAll(Map<? extends K, ? extends V> m);

    @Deprecated
    void clear();
}
