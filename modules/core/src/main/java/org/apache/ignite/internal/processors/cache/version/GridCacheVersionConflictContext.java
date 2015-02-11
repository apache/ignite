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

import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;

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

    /** Explicit TTL flag. */
    private boolean explicitTtl;

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
     * <p>
     * Note that updates from remote data centers always have explicit TTL , while local data center
     * updates will only have explicit TTL in case {@link org.apache.ignite.cache.CacheEntry#timeToLive(long)} was
     * called before update. In the latter case new entry will pick TTL of the old (existing) entry, even
     * if it was set through update from remote data center. it means that depending on concurrent
     * update timings new update might pick unexpected TTL. For example, consider that three updates
     * of the same key are performed: local update with explicit TTL (1) followed by another local
     * update without explicit TTL (2) and one remote update (3). In this case you might expect that
     * update (2) will pick TTL set during update (1). However, in case update (3) occurrs between (1)
     * and (2) and it overwrites (1) during conflict resolution, then update (2) will pick TTL of
     * update (3). To have predictable TTL in such cases you should either always set it explicitly
     * through {@code GridCacheEntry.timeToLive(long)} or use {@link #merge(Object, long)}.
     */
    public void useNew() {
        state = State.USE_NEW;

        if (!explicitTtl)
            ttl = newEntry.ttl();
    }

    /**
     * Force cache to use neither old, nor new, but some other value passed as argument. In this case old
     * value will be replaced with merge value and update will be considered as local.
     * <p>
     * Also in case of merge you have to specify new TTL explicitly. For unlimited TTL use {@code 0}.
     *
     * @param mergeVal Merge value or {@code null} to force remove.
     * @param ttl Time to live in milliseconds.
     */
    public void merge(@Nullable V mergeVal, long ttl) {
        state = State.MERGE;

        this.mergeVal = mergeVal;
        this.ttl = ttl;

        explicitTtl = true;
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
        return explicitTtl ? CU.toExpireTime(ttl) : isUseNew() ? newEntry.expireTime() :
            isUseOld() ? oldEntry.expireTime() : 0L;
    }

    /**
     * @return Explicit TTL flag.
     */
    public boolean explicitTtl() {
        return explicitTtl;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return state == State.MERGE ?
            S.toString(GridCacheVersionConflictContext.class, this, "mergeValue", mergeVal) :
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
