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
 */
public class TransactionChanges<R> {
    private static final TransactionChanges<?> EMPTY = new TransactionChanges<>(Collections.emptySet(), Collections.emptyList());

    /** All changed keys. */
    private final Set<KeyCacheObject> changedKeys;

    /** Transaction entries in required format. */
    private final List<R> newAndUpdatedEntries;

    /**
     * @param changedKeys All changed keys.
     * @param newAndUpdatedEntries New and changed entries.
     */
    public TransactionChanges(Set<KeyCacheObject> changedKeys, List<R> newAndUpdatedEntries) {
        this.changedKeys = changedKeys;
        this.newAndUpdatedEntries = newAndUpdatedEntries;
    }

    /** @return All changed keys. */
    public Set<KeyCacheObject> changedKeys() {
        return changedKeys;
    }

    /** @return New and changed entries. */
    public List<R> newAndUpdatedEntries() {
        return newAndUpdatedEntries;
    }

    /**
     * @return Empty instance.
     * @param <R> Type of new or changed row.
     */
    public static <R> TransactionChanges<R> empty() {
        return (TransactionChanges<R>)EMPTY;
    }
}
