/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;


/**
 * The base for other hash classes.
 */
public abstract class HashBase {

    /**
     * The maximum load, in percent.
     * declared as long so we do long arithmetic so we don't overflow.
     */
    private static final long MAX_LOAD = 90;

    /**
     * The bit mask to get the index from the hash code.
     */
    protected int mask;

    /**
     * The number of slots in the table.
     */
    protected int len;

    /**
     * The number of occupied slots, excluding the zero key (if any).
     */
    protected int size;

    /**
     * The number of deleted slots.
     */
    protected int deletedCount;

    /**
     * The level. The number of slots is 2 ^ level.
     */
    protected int level;

    /**
     * Whether the zero key is used.
     */
    protected boolean zeroKey;

    private int maxSize, minSize, maxDeleted;

    public HashBase() {
        reset(2);
    }

    /**
     * Increase the size of the underlying table and re-distribute the elements.
     *
     * @param newLevel the new level
     */
    protected abstract void rehash(int newLevel);

    /**
     * Get the size of the map.
     *
     * @return the size
     */
    public int size() {
        return size + (zeroKey ? 1 : 0);
    }

    /**
     * Check the size before adding an entry. This method resizes the map if
     * required.
     */
    void checkSizePut() {
        if (deletedCount > size) {
            rehash(level);
        }
        if (size + deletedCount >= maxSize) {
            rehash(level + 1);
        }
    }

    /**
     * Check the size before removing an entry. This method resizes the map if
     * required.
     */
    protected void checkSizeRemove() {
        if (size < minSize && level > 0) {
            rehash(level - 1);
        } else if (deletedCount > maxDeleted) {
            rehash(level);
        }
    }

    /**
     * Clear the map and reset the level to the specified value.
     *
     * @param newLevel the new level
     */
    protected void reset(int newLevel) {
        // can't exceed 30 or we will generate a negative value
        // for the "len" field
        if (newLevel > 30) {
            throw new IllegalStateException("exceeded max size of hash table");
        }
        size = 0;
        level = newLevel;
        len = 2 << level;
        mask = len - 1;
        minSize = (int) ((1 << level) * MAX_LOAD / 100);
        maxSize = (int) (len * MAX_LOAD / 100);
        deletedCount = 0;
        maxDeleted = 20 + len / 2;
    }

    /**
     * Calculate the index for this hash code.
     *
     * @param hash the hash code
     * @return the index
     */
    protected int getIndex(int hash) {
        return hash & mask;
    }

}
