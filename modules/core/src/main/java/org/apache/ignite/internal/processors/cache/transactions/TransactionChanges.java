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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;

/**
 * Changes of transaction in convenient for queries form.
 * @param <E> Type of new and updated entries.
 */
public class TransactionChanges<E> {
    /** Empty instance. */
    private static final TransactionChanges<?> EMPTY = new TransactionChanges<>(Collections.emptySet(), Collections.emptyList());

    /** All changed keys. */
    private final Set<KeyCacheObject> changedKeys;

    /** Transaction entries in required format. */
    private final List<E> newAndUpdatedEntries;

    /**
     * @param changedKeys All changed keys.
     * @param newAndUpdatedEntries New and changed entries.
     */
    public TransactionChanges(Set<KeyCacheObject> changedKeys, List<E> newAndUpdatedEntries) {
        this.changedKeys = changedKeys;
        this.newAndUpdatedEntries = newAndUpdatedEntries;
    }

    /**
     * @return Changed keys set.
     */
    public Set<KeyCacheObject> changedKeys() {
        return changedKeys;
    }

    /** @return New and changed entries. */
    public List<E> newAndUpdatedEntries() {
        return newAndUpdatedEntries;
    }

    /**
     * @return {@code True} is changed keys empty, {@code false} otherwise.
     */
    public boolean changedKeysEmpty() {
        return changedKeys.isEmpty();
    }

    /**
     * @param key Key to remove.
     * @return {@code True} if key removed, {@code false} otherwise.
     */
    public boolean remove(KeyCacheObject key) {
        return changedKeys.remove(key);
    }

    /**
     * @return Empty instance.
     * @param <R> Type of new or changed row.
     */
    public static <R> TransactionChanges<R> empty() {
        return (TransactionChanges<R>)EMPTY;
    }
}
