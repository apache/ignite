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

import java.util.Arrays;

/**
 * Lightweight identity hash table which maps objects to integer handles,
 * assigned in ascending order.
 */
public class GridHandleTable {
    /** Initial size. */
    private final int initCap;

    /** Factor for computing size threshold. */
    private final float loadFactor;

    /** Number of mappings in table/next available handle. */
    private int size;

    /** Size threshold determining when to expand hash spine. */
    private int threshold;

    /** Maps hash value -> candidate handle value. */
    private int[] spine;

    /** Maps handle value -> next candidate handle value. */
    private int[] next;

    /** Maps handle value -> associated object. */
    private Object[] objs;

    /**
     * Creates new HandleTable with given capacity and load factor.
     *
     * @param initCap Initial capacity.
     * @param loadFactor Load factor.
     */
    public GridHandleTable(int initCap, float loadFactor) {
        this.initCap = initCap;
        this.loadFactor = loadFactor;

        init(initCap, initCap);
    }

    /**
     * Initialize hash table.
     *
     * @param spineLen Spine array length.
     * @param size Hash table length.
     */
    private void init(int spineLen, int size) {
        spine = new int[spineLen];
        next = new int[size];
        objs = new Object[size];

        Arrays.fill(spine, -1);

        threshold = (int)(spineLen * loadFactor);
    }

    /**
     * Looks up and returns handle associated with the given object. If the given object was not found,
     * puts it into the table and returns {@code -1}.
     *
     * @param obj Object.
     * @return Object's handle or {@code -1} if it was not in the table.
     */
    public int putIfAbsent(Object obj) {
        int idx = hash(obj) % spine.length;

        if (size > 0) {
            for (int i = spine[idx]; i >= 0; i = next[i])
                if (objs[i] == obj)
                    return i;
        }

        if (size >= next.length)
            growEntries();

        if (size >= threshold) {
            growSpine();

            idx = hash(obj) % spine.length;
        }

        insert(obj, size, idx);

        size++;

        return -1;
    }

    /**
     * Resets table to its initial (empty) state.
     */
    public void clear() {
        if (size < objs.length) {
            if (shrink()) {
                size = 0;

                return;
            }
        }

        Arrays.fill(spine, -1);
        Arrays.fill(objs, 0, size, null);

        size = 0;
    }

    /**
     * @return Returns objects that were added to handles table.
     */
    public Object[] objects() {
        return objs;
    }

    /**
     * Inserts mapping object -> handle mapping into table. Assumes table
     * is large enough to accommodate new mapping.
     *
     * @param obj Object.
     * @param handle Handle.
     * @param idx Index.
     */
    private void insert(Object obj, int handle, int idx) {
        objs[handle] = obj;
        next[handle] = spine[idx];
        spine[idx] = handle;
    }

    /**
     * Expands the hash "spine" - equivalent to increasing the number of
     * buckets in a conventional hash table.
     */
    private void growSpine() {
        int size = (spine.length << 1) + 1;

        spine = new int[size];
        threshold = (int)(spine.length * loadFactor);

        Arrays.fill(spine, -1);

        for (int i = 0; i < this.size; i++) {
            Object obj = objs[i];

            int idx = hash(obj) % spine.length;

            insert(objs[i], i, idx);
        }
    }

    /**
     * Increases hash table capacity by lengthening entry arrays.
     */
    private void growEntries() {
        int newLen = (next.length << 1) + 1;
        int[] newNext = new int[newLen];

        System.arraycopy(next, 0, newNext, 0, size);

        next = newNext;

        Object[] newObjs = new Object[newLen];

        System.arraycopy(objs, 0, newObjs, 0, size);

        objs = newObjs;
    }

    /**
     * Tries to gradually shrink hash table by factor of two when it's cleared.
     *
     * @return {@code true} if shrinked the table, {@code false} otherwise.
     */
    private boolean shrink() {
        int newSize = Math.max((objs.length - 1) / 2, initCap);

        if (newSize >= size && newSize < objs.length) {
            int newSpine = spine.length;

            if (spine.length > initCap) {
                int prevSpine = (spine.length - 1) / 2;
                int prevThreshold = (int)(prevSpine * loadFactor);

                if (newSize < prevThreshold)
                    newSpine = prevSpine;
            }

            init(newSpine, newSize);

            return true;
        }

        return false;
    }

    /**
     * Returns hash value for given object.
     *
     * @param obj Object.
     * @return Hash value.
     */
    private int hash(Object obj) {
        return System.identityHashCode(obj) & 0x7FFFFFFF;
    }
}
