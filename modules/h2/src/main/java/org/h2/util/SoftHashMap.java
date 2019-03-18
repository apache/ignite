/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Map which stores items using SoftReference. Items can be garbage collected
 * and removed. It is not a general purpose cache, as it doesn't implement some
 * methods, and others not according to the map definition, to improve speed.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public class SoftHashMap<K, V> extends AbstractMap<K, V> {

    private final Map<K, SoftValue<V>> map;
    private final ReferenceQueue<V> queue = new ReferenceQueue<>();

    public SoftHashMap() {
        map = new HashMap<>();
    }

    @SuppressWarnings("unchecked")
    private void processQueue() {
        while (true) {
            Reference<? extends V> o = queue.poll();
            if (o == null) {
                return;
            }
            SoftValue<V> k = (SoftValue<V>) o;
            Object key = k.key;
            map.remove(key);
        }
    }

    @Override
    public V get(Object key) {
        processQueue();
        SoftReference<V> o = map.get(key);
        if (o == null) {
            return null;
        }
        return o.get();
    }

    /**
     * Store the object. The return value of this method is null or a
     * SoftReference.
     *
     * @param key the key
     * @param value the value
     * @return null or the old object.
     */
    @Override
    public V put(K key, V value) {
        processQueue();
        SoftValue<V> old = map.put(key, new SoftValue<>(value, queue, key));
        return old == null ? null : old.get();
    }

    /**
     * Remove an object.
     *
     * @param key the key
     * @return null or the old object
     */
    @Override
    public V remove(Object key) {
        processQueue();
        SoftReference<V> ref = map.remove(key);
        return ref == null ? null : ref.get();
    }

    @Override
    public void clear() {
        processQueue();
        map.clear();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        throw new UnsupportedOperationException();
    }

    /**
     * A soft reference that has a hard reference to the key.
     */
    private static class SoftValue<T> extends SoftReference<T> {
        final Object key;

        public SoftValue(T ref, ReferenceQueue<T> q, Object key) {
            super(ref, q);
            this.key = key;
        }

    }

}
