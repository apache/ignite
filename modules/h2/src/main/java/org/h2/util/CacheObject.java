/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import org.h2.engine.SysProperties;
import org.h2.message.DbException;

/**
 * The base object for all cached objects.
 */
public abstract class CacheObject implements Comparable<CacheObject> {

    /**
     * The previous element in the LRU linked list. If the previous element is
     * the head, then this element is the most recently used object.
     */
    public CacheObject cachePrevious;

    /**
     * The next element in the LRU linked list. If the next element is the head,
     * then this element is the least recently used object.
     */
    public CacheObject cacheNext;

    /**
     * The next element in the hash chain.
     */
    public CacheObject cacheChained;

    private int pos;
    private boolean changed;

    /**
     * Check if the object can be removed from the cache.
     * For example pinned objects can not be removed.
     *
     * @return true if it can be removed
     */
    public abstract boolean canRemove();

    /**
     * Get the estimated used memory.
     *
     * @return number of words (one word is 4 bytes)
     */
    public abstract int getMemory();

    public void setPos(int pos) {
        if (SysProperties.CHECK) {
            if (cachePrevious != null || cacheNext != null || cacheChained != null) {
                DbException.throwInternalError("setPos too late");
            }
        }
        this.pos = pos;
    }

    public int getPos() {
        return pos;
    }

    /**
     * Check if this cache object has been changed and thus needs to be written
     * back to the storage.
     *
     * @return if it has been changed
     */
    public boolean isChanged() {
        return changed;
    }

    public void setChanged(boolean b) {
        changed = b;
    }

    @Override
    public int compareTo(CacheObject other) {
        return Integer.compare(getPos(), other.getPos());
    }

    public boolean isStream() {
        return false;
    }

}
