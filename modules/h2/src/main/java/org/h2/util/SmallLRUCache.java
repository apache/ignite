/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * This class implements a small LRU object cache.
 *
 * @param <K> the key
 * @param <V> the value
 */
public class SmallLRUCache<K, V> extends LinkedHashMap<K, V> {

    private static final long serialVersionUID = 1L;
    private int size;

    private SmallLRUCache(int size) {
        super(size, (float) 0.75, true);
        this.size = size;
    }

    /**
     * Create a new object with all elements of the given collection.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param size the number of elements
     * @return the object
     */
    public static <K, V> SmallLRUCache<K, V> newInstance(int size) {
        return new SmallLRUCache<>(size);
    }

    public void setMaxSize(int size) {
        this.size = size;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        return size() > size;
    }

}
