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

package org.apache.ignite.internal.util;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Set implementation that delegates to map.
 */
public class GridSetWrapper<E> extends GridSerializableSet<E> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Dummy value. */
    protected static final Object VAL = Boolean.TRUE;

    /** Base map. */
    @GridToStringExclude
    protected Map<E, Object> map;

    /**
     * Creates new set based on the given map.
     *
     * @param map Map to be used for set implementation.
     */
    public GridSetWrapper(Map<E, ?> map) {
        A.notNull(map, "map");

        this.map = (Map<E, Object>)map;
    }

    /**
     * Creates new set based on the given map and initializes
     * it with given values.
     *
     * @param map Map to be used for set implementation.
     * @param initVals Initial values.
     */
    public GridSetWrapper(Map<E, ?> map, Collection<? extends E> initVals) {
        this(map);

        addAll(initVals);
    }

    /**
     * Provides default map value to child classes.
     *
     * @return Default map value.
     */
    protected final Object defaultValue() {
        return VAL;
    }

    /**
     * Gets wrapped map.
     *
     * @return Wrapped map.
     */
    protected final <T extends Map<E, Object>> T  map() {
        return (T)map;
    }

    /** {@inheritDoc} */
    @Override public boolean add(E e) {
        return map.put(e, VAL) == null;
    }

    /** {@inheritDoc} */
    @Override public Iterator<E> iterator() {
        return map.keySet().iterator();
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return map.size();
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        return map.isEmpty();
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"SuspiciousMethodCalls"})
    @Override public boolean contains(Object o) {
        return map.containsKey(o);
    }

    /** {@inheritDoc} */
    @Override public Object[] toArray() {
        return map.keySet().toArray();
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"SuspiciousToArrayCall"})
    @Override public <T> T[] toArray(T[] a) {
        return map.keySet().toArray(a);
    }

    /** {@inheritDoc} */
    @Override public boolean remove(Object o) {
        return map.remove(o) != null;
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        map.clear();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridSetWrapper.class, this, "elements", map.keySet());
    }
}