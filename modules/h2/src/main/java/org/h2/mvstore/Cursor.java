/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A cursor to iterate over elements in ascending order.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public class Cursor<K, V> implements Iterator<K> {
    private final K to;
    private CursorPos cursorPos;
    private CursorPos keeper;
    private K current;
    private K last;
    private V lastValue;
    private Page lastPage;

    public Cursor(Page root, K from) {
        this(root, from, null);
    }

    public Cursor(Page root, K from, K to) {
        this.cursorPos = traverseDown(root, from);
        this.to = to;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean hasNext() {
        if (cursorPos != null) {
            while (current == null) {
                Page page = cursorPos.page;
                int index = cursorPos.index;
                if (index >= (page.isLeaf() ? page.getKeyCount() : page.map.getChildPageCount(page))) {
                    CursorPos tmp = cursorPos;
                    cursorPos = cursorPos.parent;
                    tmp.parent = keeper;
                    keeper = tmp;
                    if(cursorPos == null)
                    {
                        return false;
                    }
                } else {
                    while (!page.isLeaf()) {
                        page = page.getChildPage(index);
                        if (keeper == null) {
                            cursorPos = new CursorPos(page, 0, cursorPos);
                        } else {
                            CursorPos tmp = keeper;
                            keeper = keeper.parent;
                            tmp.parent = cursorPos;
                            tmp.page = page;
                            tmp.index = 0;
                            cursorPos = tmp;
                        }
                        index = 0;
                    }
                    K key = (K) page.getKey(index);
                    if (to != null && page.map.getKeyType().compare(key, to) > 0) {
                        return false;
                    }
                    current = last = key;
                    lastValue = (V) page.getValue(index);
                    lastPage = page;
                }
                ++cursorPos.index;
            }
        }
        return current != null;
    }

    @Override
    public K next() {
        if(!hasNext()) {
            throw new NoSuchElementException();
        }
        current = null;
        return last;
    }

    /**
     * Get the last read key if there was one.
     *
     * @return the key or null
     */
    public K getKey() {
        return last;
    }

    /**
     * Get the last read value if there was one.
     *
     * @return the value or null
     */
    public V getValue() {
        return lastValue;
    }

    /**
     * Get the page where last retrieved key is located.
     *
     * @return the page
     */
    Page getPage() {
        return lastPage;
    }

    /**
     * Skip over that many entries. This method is relatively fast (for this map
     * implementation) even if many entries need to be skipped.
     *
     * @param n the number of entries to skip
     */
    public void skip(long n) {
        if (n < 10) {
            while (n-- > 0 && hasNext()) {
                next();
            }
        } else if(hasNext()) {
            assert cursorPos != null;
            CursorPos cp = cursorPos;
            CursorPos parent;
            while ((parent = cp.parent) != null) cp = parent;
            Page root = cp.page;
            @SuppressWarnings("unchecked")
            MVMap<K, ?> map = (MVMap<K, ?>) root.map;
            long index = map.getKeyIndex(next());
            last = map.getKey(index + n);
            this.cursorPos = traverseDown(root, last);
        }
    }

    @Override
    public void remove() {
        throw DataUtils.newUnsupportedOperationException(
                "Removal is not supported");
    }

    /**
     * Fetch the next entry that is equal or larger than the given key, starting
     * from the given page. This method retains the stack.
     *
     * @param p the page to start from
     * @param key the key to search, null means search for the first key
     */
    private static CursorPos traverseDown(Page p, Object key) {
        CursorPos cursorPos = null;
        while (!p.isLeaf()) {
            int index = 0;
            if(key != null) {
                index = p.binarySearch(key) + 1;
                if (index < 0) {
                    index = -index;
                }
            }
            cursorPos = new CursorPos(p, index, cursorPos);
            p = p.getChildPage(index);
        }
        int index = 0;
        if(key != null) {
            index = p.binarySearch(key);
            if (index < 0) {
                index = -index - 1;
            }
        }
        return new CursorPos(p, index, cursorPos);
    }
}
