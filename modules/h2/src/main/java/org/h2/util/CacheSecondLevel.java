/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: Jan Kotek
 */
package org.h2.util;

import java.util.ArrayList;
import java.util.Map;

/**
 * Cache which wraps another cache (proxy pattern) and adds caching using map.
 * This is useful for WeakReference, SoftReference or hard reference cache.
 */
class CacheSecondLevel implements Cache {

    private final Cache baseCache;
    private final Map<Integer, CacheObject> map;

    CacheSecondLevel(Cache cache, Map<Integer, CacheObject> map) {
        this.baseCache = cache;
        this.map = map;
    }

    @Override
    public void clear() {
        map.clear();
        baseCache.clear();
    }

    @Override
    public CacheObject find(int pos) {
        CacheObject ret = baseCache.find(pos);
        if (ret == null) {
            ret = map.get(pos);
        }
        return ret;
    }

    @Override
    public CacheObject get(int pos) {
        CacheObject ret = baseCache.get(pos);
        if (ret == null) {
            ret = map.get(pos);
        }
        return ret;
    }

    @Override
    public ArrayList<CacheObject> getAllChanged() {
        return baseCache.getAllChanged();
    }

    @Override
    public int getMaxMemory() {
        return baseCache.getMaxMemory();
    }

    @Override
    public int getMemory() {
        return baseCache.getMemory();
    }

    @Override
    public void put(CacheObject r) {
        baseCache.put(r);
        map.put(r.getPos(), r);
    }

    @Override
    public boolean remove(int pos) {
        boolean result = baseCache.remove(pos);
        result |= map.remove(pos) != null;
        return result;
    }

    @Override
    public void setMaxMemory(int size) {
        baseCache.setMaxMemory(size);
    }

    @Override
    public CacheObject update(int pos, CacheObject record) {
        CacheObject oldRec = baseCache.update(pos, record);
        map.put(pos, record);
        return oldRec;
    }

}
