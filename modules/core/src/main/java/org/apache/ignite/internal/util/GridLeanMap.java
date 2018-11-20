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

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.jetbrains.annotations.Nullable;

/**
 * Lean map implementation that keeps up to five entries in its fields.
 * {@code Null}-keys are not supported.
 */
public class GridLeanMap<K, V> extends GridSerializableMap<K, V> implements Cloneable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Implementation used internally. */
    private LeanMap<K, V> map;

    /**
     * Constructs lean map with initial size of {@code 3}.
     */
    public GridLeanMap() {
        this(3);
    }

    /**
     * Constructs lean map with initial size.
     * <p>
     * If given size is greater than zero then map activates batch mode for <tt>put()</tt>
     * operations. In this mode map allows to put up to <tt>size</tt> number of key-value
     * pairs without internal size optimization, i.e. each next <tt>put()</tt> in batch
     * doesn't switch backing implementation so that initial implementation is used as long
     * as its capacity allows.
     * <p>
     * Note that any removal operation either through iterator or map
     * itself turns batch mode off and map starts optimizing size after any modification.
     *
     * @param size Initial size.
     */
    @SuppressWarnings("IfMayBeConditional")
    public GridLeanMap(int size) {
        assert size >= 0;

        if (size == 0)
            map = null;
        else if (size == 1)
            map = new Map1<>();
        else if (size == 2)
            map = new Map2<>();
        else if (size == 3)
            map = new Map3<>();
        else if (size == 4)
            map = new Map4<>();
        else if (size == 5)
            map = new Map5<>();
        else
            map = new LeanHashMap<>(IgniteUtils.capacity(size), 0.75f);
    }

    /**
     * Constructs lean map.
     *
     * @param m Map to copy entries from.
     */
    public GridLeanMap(Map<K, V> m) {
        buildFrom(m);
    }

    /**
     * @param m Map to build from.
     */
    private void buildFrom(Map<K, V> m) {
        Iterator<Entry<K, V>> iter = m.entrySet().iterator();

        if (m.isEmpty())
            map = null;
        else if (m.size() == 1) {
            Entry<K, V> e = iter.next();

            map = new Map1<>(e.getKey(), e.getValue());
        }
        else if (m.size() == 2) {
            Entry<K, V> e1 = iter.next();
            Entry<K, V> e2 = iter.next();

            map = new Map2<>(e1.getKey(), e1.getValue(), e2.getKey(), e2.getValue());
        }
        else if (m.size() == 3) {
            Entry<K, V> e1 = iter.next();
            Entry<K, V> e2 = iter.next();
            Entry<K, V> e3 = iter.next();

            map = new Map3<>(e1.getKey(), e1.getValue(), e2.getKey(), e2.getValue(), e3.getKey(), e3.getValue());
        }
        else if (m.size() == 4) {
            Entry<K, V> e1 = iter.next();
            Entry<K, V> e2 = iter.next();
            Entry<K, V> e3 = iter.next();
            Entry<K, V> e4 = iter.next();

            map = new Map4<>(e1.getKey(), e1.getValue(), e2.getKey(), e2.getValue(), e3.getKey(), e3.getValue(),
                e4.getKey(), e4.getValue());
        }
        else if (m.size() == 5) {
            Entry<K, V> e1 = iter.next();
            Entry<K, V> e2 = iter.next();
            Entry<K, V> e3 = iter.next();
            Entry<K, V> e4 = iter.next();
            Entry<K, V> e5 = iter.next();

            map = new Map5<>(e1.getKey(), e1.getValue(), e2.getKey(), e2.getValue(), e3.getKey(), e3.getValue(),
                e4.getKey(), e4.getValue(), e5.getKey(), e5.getValue());
        }
        else
            map = new LeanHashMap<>(m);
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return map != null ? map.size() : 0;
    }

    /** {@inheritDoc} */
    @Override public boolean containsKey(Object key) {
        A.notNull(key, "key");

        return map != null && map.containsKey(key);
    }

    /** {@inheritDoc} */
    @Override public boolean containsValue(Object val) {
        return map != null && map.containsValue(val);
    }

    /** {@inheritDoc} */
    @Nullable @Override public V get(Object key) {
        A.notNull(key, "key");

        return map != null ? map.get(key) : null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public V put(K key, V val) throws NullPointerException {
        A.notNull(key, "key");

        if (map == null) {
            map = new Map1<>(key, val);

            return null;
        }

        if (!map.isFull() || map.containsKey(key))
            return map.put(key, val);

        int size = map.size();

        // Switch implementation.
        if (size == 1) {
            Map1<K, V> m = (Map1<K, V>)map;

            map = new Map2<>(m.k1, m.v1, key, val);
        }
        else if (size == 2) {
            Map2<K, V> m = (Map2<K, V>)map;

            map = new Map3<>(m.k1, m.v1, m.k2, m.v2, key, val);
        }
        else if (size == 3) {
            Map3<K, V> m = (Map3<K, V>)map;

            map = new Map4<>(m.k1, m.v1, m.k2, m.v2, m.k3, m.v3, key, val);
        }
        else if (size == 4) {
            Map4<K, V> m = (Map4<K, V>)map;

            map = new Map5<>(m.k1, m.v1, m.k2, m.v2, m.k3, m.v3, m.k4, m.v4, key, val);
        }
        else if (size == 5) {
            Map<K, V> m = map;

            map = new LeanHashMap<>(6, 1.0f);

            map.putAll(m);

            map.put(key, val);
        }
        else
            map.put(key, val);

        return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public V remove(Object key) {
        A.notNull(key, "key");

        V old;

        if (map instanceof LeanHashMap) {
            old = map.remove(key);

            if (map.size() > 5)
                return old;
            else {
                buildFrom(map);

                return old;
            }
        }
        else {
            if (map == null)
                return null;

            int size = map.size();

            old = map.remove(key);

            if (map.size() < size)
                buildFrom(map);

            return old;
        }
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        map = null;
    }

    /** {@inheritDoc} */
    @Override public Set<Entry<K, V>> entrySet() {
        return new EntrySet();
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked", "CloneDoesntDeclareCloneNotSupportedException"})
    @Override protected Object clone() {
        try {
            GridLeanMap<K, V> clone = (GridLeanMap<K, V>)super.clone();

            clone.buildFrom(this);

            return clone;
        }
        catch (CloneNotSupportedException ignore) {
            throw new InternalError();
        }
    }

    /**
     * Entry set.
     */
    private class EntrySet extends AbstractSet<Entry<K, V>> {
        @Override public Iterator<Entry<K, V>> iterator() {
            return new Iterator<Entry<K, V>>() {
                /** */
                private int idx = -1;

                /** */
                private Iterator<Entry<K, V>> mapIter;

                /** */
                private Entry<K, V> curEnt;

                /**
                 * @param forceNew If forced to create new instance.
                 * @return Iterator for internal map entry set.
                 */
                @SuppressWarnings("IfMayBeConditional")
                private Iterator<Entry<K, V>> getMapIterator(boolean forceNew) {
                    if (mapIter == null || forceNew) {
                        if (map != null)
                            mapIter = map.entrySet().iterator();
                        else {
                            mapIter = new Iterator<Entry<K, V>>() {
                                @Override public boolean hasNext() {
                                    return false;
                                }

                                @Override public Entry<K, V> next() {
                                    throw new NoSuchElementException();
                                }

                                @Override public void remove() {
                                    throw new IllegalStateException();
                                }
                            };
                        }
                    }

                    return mapIter;
                }

                @Override public boolean hasNext() {
                    return map != null && getMapIterator(false).hasNext();
                }

                @Override public Entry<K, V> next() {
                    if (!hasNext())
                        throw new NoSuchElementException();

                    idx++;

                    return curEnt = getMapIterator(false).next();
                }

                @Override public void remove() {
                    if (curEnt == null)
                        throw new IllegalStateException();

                    GridLeanMap.this.remove(curEnt.getKey());

                    curEnt = null;

                    mapIter = getMapIterator(true);

                    for (int i = 0; i < idx && mapIter.hasNext(); i++)
                        mapIter.next();

                    idx--;
                }
            };
        }

        @Override public int size() {
            return GridLeanMap.this.size();
        }
    }

    /**
     *
     */
    private interface LeanMap<K, V> extends Map<K, V> {
        /**
         * @return {@code True} if map is full.
         */
        public boolean isFull();
    }

    /**
     * Map for single entry.
     */
    private static class Map1<K, V> extends AbstractMap<K, V> implements LeanMap<K, V>, Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        protected K k1;

        /** */
        protected V v1;

        /**
         * Constructs map.
         */
        Map1() {
            // No-op.
        }

        /**
         * Constructs map.
         *
         * @param k1 Key.
         * @param v1 Value.
         */
        Map1(K k1, V v1) {
            this.k1 = k1;
            this.v1 = v1;
        }

        /** {@inheritDoc} */
        @Override public boolean isFull() {
            return size() == 1;
        }

        /** {@inheritDoc} */
        @Nullable @Override public V remove(Object key) {
            V res = null;

            if (F.eq(key, k1)) {
                res = v1;

                v1 = null;
                k1 = null;
            }

            return res;
        }

        /** {@inheritDoc} */
        @Override public int size() {
            return k1 != null ? 1 : 0;
        }

        /** {@inheritDoc} */
        @Override public boolean isEmpty() {
            return size() == 0;
        }

        /** {@inheritDoc} */
        @Override public boolean containsKey(Object key) {
            return k1 != null && F.eq(key, k1);
        }

        /** {@inheritDoc} */
        @Override public boolean containsValue(Object val) {
            return k1 != null && F.eq(val, v1);
        }

        /** {@inheritDoc} */
        @Nullable @Override public V get(Object key) {
            return k1 != null && F.eq(key, k1) ? v1 : null;
        }

        /**
         * Puts key-value pair into map only if given key is already contained in the map
         * or there are free slots.
         * Note that this implementation of {@link Map#put(Object, Object)} does not match
         * general contract of {@link Map} interface and serves only for internal purposes.
         *
         * @param key Key.
         * @param val Value.
         * @return Previous value associated with given key.
         */
        @Nullable @Override public V put(K key, V val) {
            V oldVal = get(key);

            if (k1 == null || F.eq(k1, key)) {
                k1 = key;
                v1 = val;
            }

            return oldVal;
        }

        /**
         * @param key Key.
         * @param val Value.
         * @return New entry.
         */
        protected Entry<K, V> e(K key, V val) {
            return new SimpleImmutableEntry<>(key, val);
        }

        /** {@inheritDoc} */
        @Override public Set<Entry<K, V>> entrySet() {
            return new AbstractSet<Entry<K, V>>() {
                @Override public Iterator<Entry<K, V>> iterator() {
                    return new Iterator<Entry<K, V>>() {
                        private int idx;

                        @Override public boolean hasNext() {
                            return idx == 0 && k1 != null;
                        }

                        @Override public Entry<K, V> next() {
                            if (!hasNext())
                                throw new NoSuchElementException();

                            idx = 1;

                            return e(k1, v1);
                        }

                        @Override public void remove() {
                            throw new UnsupportedOperationException();
                        }
                    };
                }

                @Override public int size() {
                    return Map1.this.size();
                }
            };
        }
    }

    /**
     * Map for two entries.
     */
    private static class Map2<K, V> extends Map1<K, V> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        protected K k2;

        /** */
        protected V v2;

        /**
         * Constructs map.
         */
        Map2() {
            // No-op.
        }

        /**
         * Constructs map.
         *
         * @param k1 Key1.
         * @param v1 Value1.
         * @param k2 Key2.
         * @param v2 Value2.
         */
        Map2(K k1, V v1, K k2, V v2) {
            super(k1, v1);

            this.k2 = k2;
            this.v2 = v2;
        }

        /** {@inheritDoc} */
        @Override public boolean isFull() {
            return size() == 2;
        }

        /** {@inheritDoc} */
        @Nullable @Override public V remove(Object key) {
            if (F.eq(key, k2)) {
                V res = v2;

                v2 = null;
                k2 = null;

                return res;
            }

            return super.remove(key);
        }

        /** {@inheritDoc} */
        @Override public int size() {
            return super.size() + (k2 != null ? 1 : 0);
        }

        /** {@inheritDoc} */
        @Override public boolean containsKey(Object k) {
            return super.containsKey(k) || (k2 != null && F.eq(k, k2));
        }

        /** {@inheritDoc} */
        @Override public boolean containsValue(Object v) {
            return super.containsValue(v) || (k2 != null && F.eq(v, v2));
        }

        /** {@inheritDoc} */
        @Override public V get(Object k) {
            V v = super.get(k);

            return v != null ? v : (k2 != null && F.eq(k, k2)) ? v2 : null;
        }

        /**
         * Puts key-value pair into map only if given key is already contained in the map
         * or there are free slots.
         * Note that this implementation of {@link Map#put(Object, Object)} does not match
         * general contract of {@link Map} interface and serves only for internal purposes.
         *
         * @param key Key.
         * @param val Value.
         * @return Previous value associated with given key.
         */
        @Nullable @Override public V put(K key, V val) throws NullPointerException {
            V oldVal = get(key);

            if (k1 == null || F.eq(k1, key)) {
                k1 = key;
                v1 = val;
            }
            else if (k2 == null || F.eq(k2, key)) {
                k2 = key;
                v2 = val;
            }

            return oldVal;
        }

        /** {@inheritDoc} */
        @Override public Set<Entry<K, V>> entrySet() {
            return new AbstractSet<Entry<K, V>>() {
                @Override public Iterator<Entry<K, V>> iterator() {
                    return new Iterator<Entry<K, V>>() {
                        private int idx;

                        private Entry<K, V> next;

                        {
                            if (k1 != null) {
                                idx = 1;
                                next = e(k1, v1);
                            }
                            else if (k2 != null) {
                                idx = 2;
                                next = e(k2, v2);
                            }
                        }

                        @Override public boolean hasNext() {
                            return next != null;
                        }

                        @Override public Entry<K, V> next() {
                            if (!hasNext())
                                throw new NoSuchElementException();

                            Entry<K, V> old = next;

                            next = null;

                            if (idx == 1 && k2 != null) {
                                idx = 2;
                                next = e(k2, v2);
                            }

                            return old;
                        }

                        @Override public void remove() {
                            throw new UnsupportedOperationException();
                        }
                    };
                }

                @Override public int size() {
                    return Map2.this.size();
                }
            };
        }
    }

    /**
     * Map for three entries.
     */
    private static class Map3<K, V> extends Map2<K, V> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        protected K k3;

        /** */
        protected V v3;

        /**
         * Constructs map.
         */
        Map3() {
            // No-op.
        }

        /**
         * Constructs map.
         *
         * @param k1 Key1.
         * @param v1 Value1.
         * @param k2 Key2.
         * @param v2 Value2.
         * @param k3 Key3.
         * @param v3 Value3.
         */
        Map3(K k1, V v1, K k2, V v2, K k3, V v3) {
            super(k1, v1, k2, v2);

            this.k3 = k3;
            this.v3 = v3;
        }

        /** {@inheritDoc} */
        @Override public boolean isFull() {
            return size() == 3;
        }

        /** {@inheritDoc} */
        @Nullable @Override public V remove(Object key) {
            if (F.eq(key, k3)) {
                V res = v3;

                v3 = null;
                k3 = null;

                return res;
            }

            return super.remove(key);
        }

        /** {@inheritDoc} */
        @Override public int size() {
            return super.size() + (k3 != null ? 1 : 0);
        }

        /** {@inheritDoc} */
        @Override public boolean containsKey(Object k) {
            return super.containsKey(k) || (k3 != null && F.eq(k, k3));
        }

        /** {@inheritDoc} */
        @Override public boolean containsValue(Object v) {
            return super.containsValue(v) || (k3 != null && F.eq(v, v3));
        }

        /** {@inheritDoc} */
        @Nullable @Override public V get(Object k) {
            V v = super.get(k);

            return v != null ? v : (k3 != null && F.eq(k, k3)) ? v3 : null;
        }

        /**
         * Puts key-value pair into map only if given key is already contained in the map
         * or there are free slots.
         * Note that this implementation of {@link Map#put(Object, Object)} does not match
         * general contract of {@link Map} interface and serves only for internal purposes.
         *
         * @param key Key.
         * @param val Value.
         * @return Previous value associated with given key.
         */
        @Nullable @Override public V put(K key, V val) throws NullPointerException {
            V oldVal = get(key);

            if (k1 == null || F.eq(k1, key)) {
                k1 = key;
                v1 = val;
            }
            else if (k2 == null || F.eq(k2, key)) {
                k2 = key;
                v2 = val;
            }
            else if (k3 == null || F.eq(k3, key)) {
                k3 = key;
                v3 = val;
            }

            return oldVal;
        }

        /** {@inheritDoc} */
        @Override public Set<Entry<K, V>> entrySet() {
            return new AbstractSet<Entry<K, V>>() {
                @Override public Iterator<Entry<K, V>> iterator() {
                    return new Iterator<Entry<K, V>>() {
                        private int idx;

                        private Entry<K, V> next;

                        {
                            if (k1 != null) {
                                idx = 1;
                                next = e(k1, v1);
                            }
                            else if (k2 != null) {
                                idx = 2;
                                next = e(k2, v2);
                            }
                            else if (k3 != null) {
                                idx = 3;
                                next = e(k3, v3);
                            }
                        }

                        @Override public boolean hasNext() {
                            return next != null;
                        }

                        @SuppressWarnings("fallthrough")
                        @Override public Entry<K, V> next() {
                            if (!hasNext())
                                throw new NoSuchElementException();

                            Entry<K, V> old = next;

                            next = null;

                            switch (idx) {
                                case 1:
                                    if (k2 != null) {
                                        idx = 2;
                                        next = e(k2, v2);

                                        break;
                                    }

                                case 2:
                                    if (k3 != null) {
                                        idx = 3;
                                        next = e(k3, v3);

                                        break;
                                    }
                            }

                            return old;
                        }

                        @Override public void remove() {
                            throw new UnsupportedOperationException();
                        }
                    };
                }

                @Override public int size() {
                    return Map3.this.size();
                }
            };
        }
    }

    /**
     * Map for four entries.
     */
    private static class Map4<K, V> extends Map3<K, V> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        protected K k4;

        /** */
        protected V v4;

        /**
         * Constructs map.
         */
        Map4() {
            // No-op.
        }

        /**
         * Constructs map.
         *
         * @param k1 Key1.
         * @param v1 Value1.
         * @param k2 Key2.
         * @param v2 Value2.
         * @param k3 Key3.
         * @param v3 Value3.
         * @param k4 Key4.
         * @param v4 Value4.
         */
        Map4(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4) {
            super(k1, v1, k2, v2, k3, v3);

            this.k4 = k4;
            this.v4 = v4;
        }

        /** {@inheritDoc} */
        @Override public boolean isFull() {
            return size() == 4;
        }

        /** {@inheritDoc} */
        @Nullable @Override public V remove(Object key) {
            if (F.eq(key, k4)) {
                V res = v4;

                v4 = null;
                k4 = null;

                return res;
            }

            return super.remove(key);
        }

        /** {@inheritDoc} */
        @Override public int size() {
            return super.size() + (k4 != null ? 1 : 0);
        }

        /** {@inheritDoc} */
        @Override public boolean containsKey(Object k) {
            return super.containsKey(k) || (k4 != null && F.eq(k, k4));
        }

        /** {@inheritDoc} */
        @Override public boolean containsValue(Object v) {
            return super.containsValue(v) || (k4 != null && F.eq(v, v4));
        }

        /** {@inheritDoc} */
        @Nullable @Override public V get(Object k) {
            V v = super.get(k);

            return v != null ? v : (k4 != null && F.eq(k, k4)) ? v4 : null;
        }

        /**
         * Puts key-value pair into map only if given key is already contained in the map
         * or there are free slots.
         * Note that this implementation of {@link Map#put(Object, Object)} does not match
         * general contract of {@link Map} interface and serves only for internal purposes.
         *
         * @param key Key.
         * @param val Value.
         * @return Previous value associated with given key.
         */
        @Nullable @Override public V put(K key, V val) throws NullPointerException {
            V oldVal = get(key);

            if (k1 == null || F.eq(k1, key)) {
                k1 = key;
                v1 = val;
            }
            else if (k2 == null || F.eq(k2, key)) {
                k2 = key;
                v2 = val;
            }
            else if (k3 == null || F.eq(k3, key)) {
                k3 = key;
                v3 = val;
            }
            else if (k4 == null || F.eq(k4, key)) {
                k4 = key;
                v4 = val;
            }

            return oldVal;
        }

        /** {@inheritDoc} */
        @Override public Set<Entry<K, V>> entrySet() {
            return new AbstractSet<Entry<K, V>>() {
                @Override public Iterator<Entry<K, V>> iterator() {
                    return new Iterator<Entry<K, V>>() {
                        private int idx;

                        private Entry<K, V> next;

                        {
                            if (k1 != null) {
                                idx = 1;
                                next = e(k1, v1);
                            }
                            else if (k2 != null) {
                                idx = 2;
                                next = e(k2, v2);
                            }
                            else if (k3 != null) {
                                idx = 3;
                                next = e(k3, v3);
                            }
                            else if (k4 != null) {
                                idx = 4;
                                next = e(k4, v4);
                            }
                        }

                        @Override public boolean hasNext() {
                            return next != null;
                        }

                        @SuppressWarnings("fallthrough")
                        @Override public Entry<K, V> next() {
                            if (!hasNext())
                                throw new NoSuchElementException();

                            Entry<K, V> old = next;

                            next = null;

                            switch (idx) {
                                case 1:
                                    if (k2 != null) {
                                        idx = 2;
                                        next = e(k2, v2);

                                        break;
                                    }

                                case 2:
                                    if (k3 != null) {
                                        idx = 3;
                                        next = e(k3, v3);

                                        break;
                                    }

                                case 3:
                                    if (k4 != null) {
                                        idx = 4;
                                        next = e(k4, v4);

                                        break;
                                    }
                            }

                            return old;
                        }

                        @Override public void remove() {
                            throw new UnsupportedOperationException();
                        }
                    };
                }

                @Override public int size() {
                    return Map4.this.size();
                }
            };
        }
    }

    /**
     * Map for five entries.
     */
    private static class Map5<K, V> extends Map4<K, V> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private K k5;

        /** */
        private V v5;

        /**
         * Constructs map.
         */
        Map5() {
            // No-op.
        }

        /**
         * Constructs map.
         *
         * @param k1 Key1.
         * @param v1 Value1.
         * @param k2 Key2.
         * @param v2 Value2.
         * @param k3 Key3.
         * @param v3 Value3.
         * @param k4 Key4.
         * @param v4 Value4.
         * @param k5 Key5.
         * @param v5 Value5.
         */
        Map5(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4, K k5, V v5) {
            super(k1, v1, k2, v2, k3, v3, k4, v4);

            this.k5 = k5;
            this.v5 = v5;
        }

        /** {@inheritDoc} */
        @Override public boolean isFull() {
            return size() == 5;
        }

        /** {@inheritDoc} */
        @Nullable @Override public V remove(Object key) {
            if (F.eq(key, k5)) {
                V res = v5;

                v5 = null;
                k5 = null;

                return res;
            }

            return super.remove(key);
        }

        /** {@inheritDoc} */
        @Override public int size() {
            return super.size() + (k5 != null ? 1 : 0);
        }

        /** {@inheritDoc} */
        @Override public boolean containsKey(Object k) {
            return super.containsKey(k) || (k5 != null && F.eq(k, k5));
        }

        /** {@inheritDoc} */
        @Override public boolean containsValue(Object v) {
            return super.containsValue(v) || (k5 != null && F.eq(v, v5));
        }

        /** {@inheritDoc} */
        @Nullable @Override public V get(Object k) {
            V v = super.get(k);

            return v != null ? v : (k5 != null && F.eq(k, k5)) ? v5 : null;
        }

        /**
         * Puts key-value pair into map only if given key is already contained in the map
         * or there are free slots.
         * Note that this implementation of {@link Map#put(Object, Object)} does not match
         * general contract of {@link Map} interface and serves only for internal purposes.
         *
         * @param key Key.
         * @param val Value.
         * @return Previous value associated with given key.
         */
        @Nullable @Override public V put(K key, V val) throws NullPointerException {
            V oldVal = get(key);

            if (k1 == null || F.eq(k1, key)) {
                k1 = key;
                v1 = val;
            }
            else if (k2 == null || F.eq(k2, key)) {
                k2 = key;
                v2 = val;
            }
            else if (k3 == null || F.eq(k3, key)) {
                k3 = key;
                v3 = val;
            }
            else if (k4 == null || F.eq(k4, key)) {
                k4 = key;
                v4 = val;
            }
            else if (k5 == null || F.eq(k5, key)) {
                k5 = key;
                v5 = val;
            }

            return oldVal;
        }

        /** {@inheritDoc} */
        @Override public Set<Entry<K, V>> entrySet() {
            return new AbstractSet<Entry<K, V>>() {
                @Override public Iterator<Entry<K, V>> iterator() {
                    return new Iterator<Entry<K, V>>() {
                        private int idx;

                        private Entry<K, V> next;

                        {
                            if (k1 != null) {
                                idx = 1;
                                next = e(k1, v1);
                            }
                            else if (k2 != null) {
                                idx = 2;
                                next = e(k2, v2);
                            }
                            else if (k3 != null) {
                                idx = 3;
                                next = e(k3, v3);
                            }
                            else if (k4 != null) {
                                idx = 4;
                                next = e(k4, v4);
                            }
                            else if (k5 != null) {
                                idx = 5;
                                next = e(k5, v5);
                            }
                        }

                        @Override public boolean hasNext() {
                            return next != null;
                        }

                        @SuppressWarnings("fallthrough")
                        @Override public Entry<K, V> next() {
                            if (!hasNext())
                                throw new NoSuchElementException();

                            Entry<K, V> old = next;

                            next = null;

                            switch (idx) {
                                case 1:
                                    if (k2 != null) {
                                        idx = 2;
                                        next = e(k2, v2);

                                        break;
                                    }

                                case 2:
                                    if (k3 != null) {
                                        idx = 3;
                                        next = e(k3, v3);

                                        break;
                                    }

                                case 3:
                                    if (k4 != null) {
                                        idx = 4;
                                        next = e(k4, v4);

                                        break;
                                    }

                                case 4:
                                    if (k5 != null) {
                                        idx = 5;
                                        next = e(k5, v5);

                                        break;
                                    }
                            }

                            return old;
                        }

                        @Override public void remove() {
                            throw new UnsupportedOperationException();
                        }
                    };
                }

                @Override public int size() {
                    return Map5.this.size();
                }
            };
        }
    }

    /**
     *
     */
    private static class LeanHashMap<K, V> extends HashMap<K, V> implements LeanMap<K, V> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param initCap Capacity.
         * @param loadFactor Load factor.
         */
        private LeanHashMap(int initCap, float loadFactor) {
            super(initCap, loadFactor);
        }

        /**
         * @param m Map.
         */
        private LeanHashMap(Map<? extends K, ? extends V> m) {
            super(m);
        }

        /** {@inheritDoc} */
        @Override public boolean isFull() {
            return false;
        }
    }
}