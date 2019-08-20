/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.util.ArrayList;

/**
 * An alternative cache implementation. This implementation uses two caches: a
 * LRU cache and a FIFO cache. Entries are first kept in the FIFO cache, and if
 * referenced again then marked in a hash set. If referenced again, they are
 * moved to the LRU cache. Stream pages are never added to the LRU cache. It is
 * supposed to be more or less scan resistant, and it doesn't cache large rows
 * in the LRU cache.
 */
public class CacheTQ implements Cache {

    static final String TYPE_NAME = "TQ";

    private final Cache lru;
    private final Cache fifo;
    private final SmallLRUCache<Integer, Object> recentlyUsed =
            SmallLRUCache.newInstance(1024);
    private int lastUsed = -1;

    private int maxMemory;

    CacheTQ(CacheWriter writer, int maxMemoryKb) {
        this.maxMemory = maxMemoryKb;
        lru = new CacheLRU(writer, (int) (maxMemoryKb * 0.8), false);
        fifo = new CacheLRU(writer, (int) (maxMemoryKb * 0.2), true);
        setMaxMemory(4 * maxMemoryKb);
    }

    @Override
    public void clear() {
        lru.clear();
        fifo.clear();
        recentlyUsed.clear();
        lastUsed = -1;
    }

    @Override
    public CacheObject find(int pos) {
        CacheObject r = lru.find(pos);
        if (r == null) {
            r = fifo.find(pos);
        }
        return r;
    }

    @Override
    public CacheObject get(int pos) {
        CacheObject r = lru.find(pos);
        if (r != null) {
            return r;
        }
        r = fifo.find(pos);
        if (r != null && !r.isStream()) {
            if (recentlyUsed.get(pos) != null) {
                if (lastUsed != pos) {
                    fifo.remove(pos);
                    lru.put(r);
                }
            } else {
                recentlyUsed.put(pos, this);
            }
            lastUsed = pos;
        }
        return r;
    }

    @Override
    public ArrayList<CacheObject> getAllChanged() {
        ArrayList<CacheObject> changed = New.arrayList();
        changed.addAll(lru.getAllChanged());
        changed.addAll(fifo.getAllChanged());
        return changed;
    }

    @Override
    public int getMaxMemory() {
        return maxMemory;
    }

    @Override
    public int getMemory() {
        return lru.getMemory() + fifo.getMemory();
    }

    @Override
    public void put(CacheObject r) {
        if (r.isStream()) {
            fifo.put(r);
        } else if (recentlyUsed.get(r.getPos()) != null) {
            lru.put(r);
        } else {
            fifo.put(r);
            lastUsed = r.getPos();
        }
    }

    @Override
    public boolean remove(int pos) {
        boolean result = lru.remove(pos);
        if (!result) {
            result = fifo.remove(pos);
        }
        recentlyUsed.remove(pos);
        return result;
    }

    @Override
    public void setMaxMemory(int maxMemoryKb) {
        this.maxMemory = maxMemoryKb;
        lru.setMaxMemory((int) (maxMemoryKb * 0.8));
        fifo.setMaxMemory((int) (maxMemoryKb * 0.2));
        recentlyUsed.setMaxSize(4 * maxMemoryKb);
    }

    @Override
    public CacheObject update(int pos, CacheObject record) {
        if (lru.find(pos) != null) {
            return lru.update(pos, record);
        }
        return fifo.update(pos, record);
    }

}
