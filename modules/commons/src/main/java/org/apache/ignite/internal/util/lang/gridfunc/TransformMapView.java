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
import org.apache.ignite.lang.IgniteClosure;
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
public class TransformMapView<K, V1, V> extends GridSerializableMap<K, V1> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final Map<K, V> map;

    /** */
    private final IgniteClosure<V, V1> clos;

    /** */
    private final boolean hasPred;

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
    public TransformMapView(Map<K, V> map, IgniteClosure<V, V1> clos,
        IgnitePredicate<? super K>... preds) {
        this.map = map;
        this.clos = clos;
        this.hasPred = (preds != null && preds.length > 0);
        this.preds = preds;
        this.entryPred = new EntryByKeyEvaluationPredicate(preds);
    }

    /** {@inheritDoc} */
    @NotNull @Override public Set<Entry<K, V1>> entrySet() {
        return new GridSerializableSet<Entry<K, V1>>() {
            @NotNull
            @Override public Iterator<Entry<K, V1>> iterator() {
                return new Iterator<Entry<K, V1>>() {
                    private Iterator<Entry<K, V>> iter = GridFunc.iterator0(map.entrySet(), true, entryPred);

                    @Override public boolean hasNext() {
                        return iter.hasNext();
                    }

                    @Override public Entry<K, V1> next() {
                        final Entry<K, V> e = iter.next();

                        return new Entry<K, V1>() {
                            @Override public K getKey() {
                                return e.getKey();
                            }

                            @Override public V1 getValue() {
                                return clos.apply(e.getValue());
                            }

                            @Override public V1 setValue(V1 val) {
                                throw new UnsupportedOperationException("Put is not supported for readonly map view.");
                            }
                        };
                    }

                    @Override public void remove() {
                        throw new UnsupportedOperationException("Remove is not support for readonly map view.");
                    }
                };
            }

            @Override public int size() {
                return hasPred ? F.size(map.keySet(), preds) : map.size();
            }

            @Override public boolean remove(Object o) {
                throw new UnsupportedOperationException("Remove is not support for readonly map view.");
            }

            @Override public boolean contains(Object o) {
                return F.isAll((Entry<K, V>)o, entryPred) && map.entrySet().contains(o);
            }

            @Override public boolean isEmpty() {
                return hasPred ? !iterator().hasNext() : map.isEmpty();
            }
        };
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        return hasPred ? entrySet().isEmpty() : map.isEmpty();
    }

    /** {@inheritDoc} */
    @Nullable @Override public V1 get(Object key) {
        if (GridFunc.isAll((K)key, preds)) {
            V v = map.get(key);

            if (v != null)
                return clos.apply(v);
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
}
