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

import java.util.*;

/**
 * Interface for ignite internal tree.
 */
public interface IgniteTree<K, V> {
    /**
     * Associates the specified value with the specified key in this tree.
     *
     * @param key key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     * @return the previous value associated with key
     */
    V put(K key, V value);

    /**
     * Returns the value to which the specified key is mapped, or {@code null} if this tree contains no mapping for the
     * key.
     *
     * @param key the key whose associated value is to be returned
     * @return the value to which the specified key is mapped, or {@code null} if this tree contains no mapping for the
     * key
     */
    V get(Object key);

    /**
     * Removes the mapping for a key from this tree if it is present.
     *
     * @param key key whose mapping is to be removed from the tree
     * @return the previous value associated with key, or null if there was no mapping for key.
     */
    V remove(Object key);

    /**
     * Returns the number of elements in this tree.
     *
     * @return the number of elements in this tree
     */
    int size();

    /**
     * Returns a {@link Collection} view of the values contained in this tree.
     *
     * @return a collection view of the values contained in this map
     */
    Collection<V> values();

    /**
     * Returns a view of the portion of this tree whose keys are less than (or equal to, if {@code inclusive} is true)
     * {@code toKey}.  The returned tree is backed by this tree, so changes in the returned tree are reflected in this
     * tree, and vice-versa.  The returned tree supports all optional tree operations that this tree supports.
     *
     * @param toKey high endpoint of the keys in the returned tree
     * @param inclusive {@code true} if the high endpoint is to be included in the returned view
     * @return a view of the portion of this tree whose keys are less than (or equal to, if {@code inclusive} is true)
     * {@code toKey}
     */
    IgniteTree<K, V> headTree(K toKey, boolean inclusive);

    /**
     * Returns a view of the portion of this tree whose keys are greater than (or equal to, if {@code inclusive} is
     * true) {@code fromKey}.  The returned tree is backed by this tree, so changes in the returned tree are reflected
     * in this tree, and vice-versa.  The returned tree supports all optional tree operations that this tree supports.
     *
     * @param fromKey low endpoint of the keys in the returned tree
     * @param inclusive {@code true} if the low endpoint is to be included in the returned view
     * @return a view of the portion of this tree whose keys are greater than (or equal to, if {@code inclusive} is
     * true) {@code fromKey}
     */
    IgniteTree<K, V> tailTree(K fromKey, boolean inclusive);

    /**
     * Returns a view of the portion of this tree whose keys range from {@code fromKey} to {@code toKey}.  If {@code
     * fromKey} and {@code toKey} are equal, the returned tree is empty unless {@code fromInclusive} and {@code
     * toInclusive} are both true.  The returned tree is backed by this tree, so changes in the returned tree are
     * reflected in this tree, and vice-versa.  The returned tree supports all optional tree operations that this tree
     * supports.
     *
     * @param fromKey low endpoint of the keys in the returned tree
     * @param fromInclusive {@code true} if the low endpoint is to be included in the returned view
     * @param toKey high endpoint of the keys in the returned tree
     * @param toInclusive {@code true} if the high endpoint is to be included in the returned view
     * @return a view of the portion of this tree whose keys range from {@code fromKey} to {@code toKey}
     */
    IgniteTree<K, V> subTree(final K fromKey, final boolean fromInclusive, final K toKey, final boolean toInclusive);
}
