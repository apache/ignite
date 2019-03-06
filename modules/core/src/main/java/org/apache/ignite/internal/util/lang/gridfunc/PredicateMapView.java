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

package org.apache.ignite.internal.util.lang.gridfunc;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.util.GridSerializableMap;
import org.apache.ignite.internal.util.GridSerializableSet;
import org.apache.ignite.internal.util.lang.GridFunc;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Light-weight view on given map with provided predicate.
 *
 * @param <K> Type of the key.
 * @param <V> Type of the value.
 */
public class PredicateMapView<K, V> extends GridSerializableMap<K, V> {
    /** */
    private static final long serialVersionUID = 5531745605372387948L;

    /** */
    private final Map<K, V> map;

    /** */
    private final IgnitePredicate<? super K>[] preds;

    /** Entry predicate. */
    private IgnitePredicate<Entry<K, V>> entryPred;

    /**
     * @param map Input map that serves as a base for the view.
     * @param preds Optional predicates. If predicates are not provided - all will be in the view.
     */
    @SuppressWarnings({"unchecked"})
    public PredicateMapView(Map<K, V> map, IgnitePredicate<? super K>... preds) {
        this.map = map;
        this.preds = preds;
        this.entryPred = new EntryByKeyEvaluationPredicate(preds);
    }

    /** {@inheritDoc} */
    @NotNull @Override public Set<Entry<K, V>> entrySet() {
        return new GridSerializableSet<Entry<K, V>>() {
            @NotNull
            @Override public Iterator<Entry<K, V>> iterator() {
                return GridFunc.iterator0(map.entrySet(), false, entryPred);
            }

            @Override public int size() {
                return F.size(map.keySet(), preds);
            }

            @Override public boolean remove(Object o) {
                return F.isAll((Entry<K, V>)o, entryPred) && map.entrySet().remove(o);
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
    @Nullable @Override public V get(Object key) {
        return GridFunc.isAll((K)key, preds) ? map.get(key) : null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public V put(K key, V val) {
        V oldVal = get(key);

        if (GridFunc.isAll(key, preds))
            map.put(key, val);

        return oldVal;
    }

    /** {@inheritDoc} */
    @Override public boolean containsKey(Object key) {
        return GridFunc.isAll((K)key, preds) && map.containsKey(key);
    }
}
