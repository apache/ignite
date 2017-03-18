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

package org.apache.ignite.internal.util.lang.gridfunc;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.util.GridSerializableMap;
import org.apache.ignite.internal.util.GridSerializableSet;
import org.apache.ignite.internal.util.lang.GridFunc;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgnitePredicate;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Light-weight view on given map with provided predicate.
 *
 * @param <K> Type of the key.
 * @param <V> Type of the value.
 */
public class GridSerializableMapPredicatesWrapper<K, V> extends GridSerializableMap<K, V> {
    /** */
    private static final long serialVersionUID = 5531745605372387948L;

    /** */
    private final Map<K, V> map;

    /** */
    private final IgnitePredicate<? super K>[] predicates;

    /** Entry predicate. */
    private IgnitePredicate<Entry<K, V>> entryPredicate;

    /**
     * @param map Input map that serves as a base for the view.
     * @param predicates Optional predicates. If predicates are not provided - all will be in the view.
     */
    @SuppressWarnings({"unchecked"})
    public GridSerializableMapPredicatesWrapper(Map<K, V> map, IgnitePredicate<? super K>... predicates) {
        this.map = map;
        this.predicates = predicates;
        this.entryPredicate = new IgnitePredicateEvaluateEntryByKey(predicates);
    }

    /** {@inheritDoc} */
    @NotNull @Override public Set<Entry<K, V>> entrySet() {
        return new GridSerializableSet<Entry<K, V>>() {
            @NotNull
            @Override public Iterator<Entry<K, V>> iterator() {
                return GridFunc.iterator0(map.entrySet(), false, entryPredicate);
            }

            @Override public int size() {
                return F.size(map.keySet(), predicates);
            }

            @SuppressWarnings({"unchecked"})
            @Override public boolean remove(Object o) {
                return F.isAll((Entry<K, V>)o, entryPredicate) && map.entrySet().remove(o);
            }

            @SuppressWarnings({"unchecked"})
            @Override public boolean contains(Object o) {
                return F.isAll((Entry<K, V>)o, entryPredicate) && map.entrySet().contains(o);
            }

            @Override public boolean isEmpty() {
                return !iterator().hasNext();
            }
        };
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        return entrySet().isEmpty();
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Nullable @Override public V get(Object key) {
        return GridFunc.isAll((K)key, predicates) ? map.get(key) : null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public V put(K key, V val) {
        V oldVal = get(key);

        if (GridFunc.isAll(key, predicates))
            map.put(key, val);

        return oldVal;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override public boolean containsKey(Object key) {
        return GridFunc.isAll((K)key, predicates) && map.containsKey(key);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridSerializableMapPredicatesWrapper.class, this);
    }
}
