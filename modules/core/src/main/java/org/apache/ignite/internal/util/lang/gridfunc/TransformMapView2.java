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
import org.apache.ignite.lang.IgniteBiClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Light-weight view on given map with provided predicate and clos.
 *
 * @param <K> Type of the key.
 * @param <V> Type of the input map value.
 * @param <V1> Type of the output map value.
 */
public class TransformMapView2<K, V, V1> extends GridSerializableMap<K, V1> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final Map<K, V> map;

    /** */
    private final IgniteBiClosure<K, V, V1> clos;

    /** */
    private final IgnitePredicate<? super K>[] preds;

    /** Entry predicate. */
    private IgnitePredicate<Entry<K, V>> entryPred;

    /**
     * @param map Input map that serves as a base for the view.
     * @param clos Transformer for map value transformation.
     * @param preds Optional predicates. If predicates are not provided - all will be in the view.
     */
    @SuppressWarnings({"unchecked"})
    public TransformMapView2(Map<K, V> map, IgniteBiClosure<K, V, V1> clos,
        IgnitePredicate<? super K>... preds) {
        this.map = map;
        this.clos = clos;
        this.preds = preds;
        entryPred = new EntryByKeyEvaluationPredicate(preds);
    }

    /** {@inheritDoc} */
    @NotNull @Override public Set<Entry<K, V1>> entrySet() {
        return new GridSerializableSet<Entry<K, V1>>() {
            @NotNull
            @Override public Iterator<Entry<K, V1>> iterator() {
                return new Iterator<Entry<K, V1>>() {
                    private Iterator<Entry<K, V>> it = GridFunc.iterator0(map.entrySet(), true, entryPred);

                    @Override public boolean hasNext() {
                        return it.hasNext();
                    }

                    @Override public Entry<K, V1> next() {
                        final Entry<K, V> e = it.next();

                        return new Entry<K, V1>() {
                            @Override public K getKey() {
                                return e.getKey();
                            }

                            @Override public V1 getValue() {
                                return clos.apply(e.getKey(), e.getValue());
                            }

                            @Override public V1 setValue(V1 val) {
                                throw new UnsupportedOperationException(
                                    "Put is not supported for readonly map view.");
                            }
                        };
                    }

                    @Override public void remove() {
                        throw new UnsupportedOperationException("Remove is not support for readonly map view.");
                    }
                };
            }

            @Override public int size() {
                return F.size(map.keySet(), preds);
            }

            @Override public boolean remove(Object o) {
                throw new UnsupportedOperationException("Remove is not support for readonly map view.");
            }

            @Override public boolean contains(Object o) {
                return F.isAll((Entry<K, V>)o, entryPred) && map.entrySet().contains(o);
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
    @Nullable @Override public V1 get(Object key) {
        if (GridFunc.isAll((K)key, preds)) {
            V v = map.get(key);

            if (v != null)
                return clos.apply((K)key, v);
        }

        return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public V1 put(K key, V1 val) {
        throw new UnsupportedOperationException("Put is not supported for readonly map view.");
    }

    /** {@inheritDoc} */
    @Override public V1 remove(Object key) {
        throw new UnsupportedOperationException("Remove is not supported for readonly map view.");
    }

    /** {@inheritDoc} */
    @Override public boolean containsKey(Object key) {
        return GridFunc.isAll((K)key, preds) && map.containsKey(key);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TransformMapView2.class, this);
    }
}
