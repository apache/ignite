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

import org.apache.ignite.cache.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;

/**
 * Conflict context implementation.
 */
public class GridCacheVersionConflictContextImpl<K, V> implements GridCacheVersionConflictContext<K, V> {
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
    public GridCacheVersionConflictContextImpl(GridCacheVersionedEntry<K, V> oldEntry,
        GridCacheVersionedEntry<K, V> newEntry) {
        assert oldEntry != null && newEntry != null;
        assert oldEntry.ttl() >= 0 && newEntry.ttl() >= 0;

        this.oldEntry = oldEntry;
        this.newEntry = newEntry;

        // Set initial state.
        useNew();
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersionedEntry<K, V> oldEntry() {
        return oldEntry;
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersionedEntry<K, V> newEntry() {
        return newEntry;
    }

    /** {@inheritDoc} */
    @Override public void useOld() {
        state = State.USE_OLD;
    }

    /** {@inheritDoc} */
    @Override public void useNew() {
        state = State.USE_NEW;

        if (!explicitTtl)
            ttl = newEntry.ttl();
    }

    /** {@inheritDoc} */
    @Override public void merge(@Nullable V mergeVal, long ttl) {
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
            S.toString(GridCacheVersionConflictContextImpl.class, this, "mergeValue", mergeVal) :
            S.toString(GridCacheVersionConflictContextImpl.class, this);
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
