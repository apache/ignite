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

package org.apache.ignite.internal.util.collection;

/**
 * The map for integer keys.
 */
public interface IntMap<V> {
    /***
     * The bridge for consuming all entries of the map.
     */
    public interface EntryConsumer<V, E extends Throwable> {
        /**
         * @param key entry key.
         * @param val store value.
         */
        void accept(int key, V val) throws E;
    }

    /**
     * Returns <tt>true</tt> if the map contains the key, otherwise <tt>false</tt>.
     * @param key tests key value.
     */
    boolean containsKey(int key);

    /**
     * Returns <tt>true</tt> if the map contains one or more values, otherwise <tt>false</tt>.
     * @param val value to be associated with the specified key.
     */
    boolean containsValue(V val);

    /**
     * Returns value associated with the key. if the map doesn't contain the key, returns null.
     * @param key key with which the specified value is to be associated.
     */
    V get(int key);

    /**
     * Save the pair into the map. If a pair is present, returns old value and store new.
     * @param key key with which the specified value is to be associated.
     * @param val value to be associated with the specified key.
     */
    V put(int key, V val);

    /**
     * @param key key with which the specified value is to be associated.
     */
    V remove(int key);

    /**
     * Does put into the map if a pair isn't present, otherwise returns stored value.
     * @param key key with which the specified value is to be associated..
     * @param val value to be associated with the specified key..
     */
    V putIfAbsent(int key, V val);

    /**
     * This method work under a read lock, be careful with long operations inside.
     * @param act Action.
     */
    <E extends Throwable> void forEach(EntryConsumer<V, E> act) throws E;

    /** Returns count of elements. */
    int size();

    /**
     * Returns <tt>true</tt> if this map contains no key-value mappings.
     */
    boolean isEmpty();
}
