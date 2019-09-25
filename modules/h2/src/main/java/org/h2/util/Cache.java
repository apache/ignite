/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.util.ArrayList;

/**
 * The cache keeps frequently used objects in the main memory.
 */
public interface Cache {

    /**
     * Get all objects in the cache that have been changed.
     *
     * @return the list of objects
     */
    ArrayList<CacheObject> getAllChanged();

    /**
     * Clear the cache.
     */
    void clear();

    /**
     * Get an element in the cache if it is available.
     * This will move the item to the front of the list.
     *
     * @param pos the unique key of the element
     * @return the element or null
     */
    CacheObject get(int pos);

    /**
     * Add an element to the cache. Other items may fall out of the cache
     * because of this. It is not allowed to add the same record twice.
     *
     * @param r the object
     */
    void put(CacheObject r);

    /**
     * Update an element in the cache.
     * This will move the item to the front of the list.
     *
     * @param pos the unique key of the element
     * @param record the element
     * @return the element
     */
    CacheObject update(int pos, CacheObject record);

    /**
     * Remove an object from the cache.
     *
     * @param pos the unique key of the element
     * @return true if the key was in the cache
     */
    boolean remove(int pos);

    /**
     * Get an element from the cache if it is available.
     * This will not move the item to the front of the list.
     *
     * @param pos the unique key of the element
     * @return the element or null
     */
    CacheObject find(int pos);

    /**
     * Set the maximum memory to be used by this cache.
     *
     * @param size the maximum size in KB
     */
    void setMaxMemory(int size);

    /**
     * Get the maximum memory to be used.
     *
     * @return the maximum size in KB
     */
    int getMaxMemory();

    /**
     * Get the used size in KB.
     *
     * @return the current size in KB
     */
    int getMemory();

}
