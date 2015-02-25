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
import java.util.Map.*;


/**
 * A static convenience class for creating efficient persistent maps.
 * <p/>
 * This class simply creates HashPMaps backed by IntTreePMaps.
 *
 * @author harold
 */
public final class HashTreePMap {
    private static final HashPMap<Object, Object> EMPTY
        = HashPMap.empty(IntTreePMap.<PSequence<Entry<Object, Object>>>empty());

    // not instantiable (or subclassable):
    private HashTreePMap() {
    }

    /**
     * @param <K>
     * @param <V>
     * @return an empty map
     */
    @SuppressWarnings("unchecked")
    public static <K, V> HashPMap<K, V> empty() {
        return (HashPMap<K, V>) EMPTY;
    }

    /**
     * @param <K>
     * @param <V>
     * @param key
     * @param value
     * @return empty().plus(key, value)
     */
    public static <K, V> HashPMap<K, V> singleton(final K key, final V value) {
        return HashTreePMap.<K, V>empty().plus(key, value);
    }

    /**
     * @param <K>
     * @param <V>
     * @param map
     * @return empty().plusAll(map)
     */
    public static <K, V> HashPMap<K, V> from(final Map<? extends K, ? extends V> map) {
        return HashTreePMap.<K, V>empty().plusAll(map);
    }
}
