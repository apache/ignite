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

package org.apache.ignite.internal.client.util;

import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Wrapper around concurrent map.
 */
public class GridConcurrentHashSet<E> extends AbstractSet<E> {
    /** Dummy value. */
    protected static final Object VAL = Boolean.TRUE;

    /** Base map. */
    protected ConcurrentMap<E, Object> map;

    /**
     * Creates new set based on {@link ConcurrentHashMap}.
     */
    public GridConcurrentHashSet() {
        this(new ConcurrentHashMap<E, Object>());
    }

    /**
     * Creates new set based on the given map.
     *
     * @param map Map to be used for set implementation.
     */
    @SuppressWarnings({"unchecked"})
    public GridConcurrentHashSet(ConcurrentMap<E, ?> map) {
        this.map = (ConcurrentMap<E, Object>)map;
    }

    /**
     * Gets wrapped map.
     *
     * @return Wrapped map.
     */
    @SuppressWarnings({"unchecked"})
    protected final <T extends Map<E, Object>> T  map() {
        return (T)map;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public boolean add(E e) {
        return map.put(e, VAL) == null;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
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
        return map.keySet().toString();
    }
}