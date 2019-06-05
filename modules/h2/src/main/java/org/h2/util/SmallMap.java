/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.util.HashMap;
import java.util.Iterator;

import org.h2.api.ErrorCode;
import org.h2.message.DbException;

/**
 * A simple hash table with an optimization for the last recently used object.
 */
public class SmallMap {

    private final HashMap<Integer, Object> map = new HashMap<>();
    private Object cache;
    private int cacheId;
    private int lastId;
    private final int maxElements;

    /**
     * Create a map with the given maximum number of entries.
     *
     * @param maxElements the maximum number of entries
     */
    public SmallMap(int maxElements) {
        this.maxElements = maxElements;
    }

    /**
     * Add an object to the map. If the size of the map is larger than twice the
     * maximum size, objects with a low id are removed.
     *
     * @param id the object id
     * @param o the object
     * @return the id
     */
    public int addObject(int id, Object o) {
        if (map.size() > maxElements * 2) {
            Iterator<Integer> it = map.keySet().iterator();
            while (it.hasNext()) {
                Integer k = it.next();
                if (k.intValue() + maxElements < lastId) {
                    it.remove();
                }
            }
        }
        if (id > lastId) {
            lastId = id;
        }
        map.put(id, o);
        cacheId = id;
        cache = o;
        return id;
    }

    /**
     * Remove an object from the map.
     *
     * @param id the id of the object to remove
     */
    public void freeObject(int id) {
        if (cacheId == id) {
            cacheId = -1;
            cache = null;
        }
        map.remove(id);
    }

    /**
     * Get an object from the map if it is stored.
     *
     * @param id the id of the object
     * @param ifAvailable only return it if available, otherwise return null
     * @return the object or null
     * @throws DbException if isAvailable is false and the object has not been
     *             found
     */
    public Object getObject(int id, boolean ifAvailable) {
        if (id == cacheId) {
            return cache;
        }
        Object obj = map.get(id);
        if (obj == null && !ifAvailable) {
            throw DbException.get(ErrorCode.OBJECT_CLOSED);
        }
        return obj;
    }

}
