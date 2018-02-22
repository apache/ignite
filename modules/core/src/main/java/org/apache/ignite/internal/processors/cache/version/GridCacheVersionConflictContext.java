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

package org.apache.ignite.internal.processors.cache.version;

import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Conflict context implementation.
 */
public class GridCacheVersionConflictContext<K, V> {
    /** Old entry. */
    @GridToStringInclude
    private final GridCacheVersionedEntry<K, V> oldEntry;

    /** New entry. */
    @GridToStringInclude
    private final GridCacheVersionedEntry<K, V> newEntry;

    /** Current state. */
    private State state;

    /** Current merge value. */
    @GridToStringExclude
    private V mergeVal;

    /** TTL. */
    private long ttl;

    /** Manual resolve flag. */
    private boolean manualResolve;

    /**
     * Constructor.
     *
     * @param oldEntry Old entry.
     * @param newEntry New entry.
     */
    public GridCacheVersionConflictContext(GridCacheVersionedEntry<K, V> oldEntry,
        GridCacheVersionedEntry<K, V> newEntry) {
        assert oldEntry != null && newEntry != null;
        assert oldEntry.ttl() >= 0 && newEntry.ttl() >= 0;

        this.oldEntry = oldEntry;
        this.newEntry = newEntry;

        // Set initial state.
        useNew();
    }

    /**
     * Gets old (existing) cache entry.
     *
     * @return Old (existing) cache entry.
     */
    public GridCacheVersionedEntry<K, V> oldEntry() {
        return oldEntry;
    }

    /**
     * Gets new cache entry.
     *
     * @return New cache entry.
     */
    public GridCacheVersionedEntry<K, V> newEntry() {
        return newEntry;
    }

    /**
     * Force cache to ignore new entry and leave old (existing) entry unchanged.
     */
    public void useOld() {
        state = State.USE_OLD;
    }

    /**
     * Force cache to apply new entry overwriting old (existing) entry.
     */
    public void useNew() {
        state = State.USE_NEW;

        ttl = newEntry.ttl();
    }

    /**
     * Force cache to use neither old, nor new, but some other value passed as argument. In this case old
     * value will be replaced with merge value and update will be considered as local.
     * <p>
     * Also in case of merge you have to specify new TTL explicitly. For unlimited TTL use {@code 0}.
     *
     * @param mergeVal Merge value or {@code null} to force remove.
     * @param ttl Time to live in milliseconds (must be non-negative).
     */
    public void merge(@Nullable V mergeVal, long ttl) {
        if (ttl < 0)
            throw new IllegalArgumentException("TTL must be non-negative: " + ttl);

        state = State.MERGE;

        this.mergeVal = mergeVal;
        this.ttl = ttl;
    }

    /**
     * @return {@code True} in case old value should be used.
     */
    public boolean isUseOld() {
        return state == State.USE_OLD;
    }

    /**
     * @return {@code True} in case new value should be used.
     */
    public boolean isUseNew() {
        return state == State.USE_NEW;
    }

    /**
     * @return {@code True} in case merge is to be performed.
     */
    public boolean isMerge() {
        return state == State.MERGE;
    }

    /**
     * Set manual resolve class.
     */
    public void manualResolve() {
        this.manualResolve = true;
    }

    /**
     * @return Manual resolve flag.
     */
    public boolean isManualResolve() {
        return manualResolve;
    }

    /**
     * @return Value to merge (if any).
     */
    @Nullable public V mergeValue() {
        return mergeVal;
    }

    /**
     * @return TTL.
     */
    public long ttl() {
        return ttl;
    }

    /**
     * @return Expire time.
     */
    public long expireTime() {
        return isUseNew() ? newEntry.expireTime() : isUseOld() ? oldEntry.expireTime() : CU.toExpireTime(ttl);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return state == State.MERGE ?
            S.toString(GridCacheVersionConflictContext.class, this, "mergeValue", mergeVal, true) :
            S.toString(GridCacheVersionConflictContext.class, this);
    }

    /**
     * State.
     */
    private enum State {
        /** Use old. */
        USE_OLD,

        /** Use new. */
        USE_NEW,

        /** Merge. */
        MERGE
    }
}