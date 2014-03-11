/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.dr;

import org.gridgain.grid.dr.*;
import org.gridgain.grid.dr.cache.receiver.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;
import org.jetbrains.annotations.*;

/**
 * Data center replication conflict context implementation.
 */
public class GridDrReceiverConflictContextImpl<K, V> implements GridDrReceiverCacheConflictContext<K, V> {
    /** Old entry. */
    @GridToStringInclude
    private final GridDrEntry<K, V> oldEntry;

    /** New entry. */
    @GridToStringInclude
    private final GridDrEntry<K, V> newEntry;

    /** Current state. */
    private State state;

    /** Current merge value. */
    @GridToStringExclude
    private V mergeVal;

    /** TTL. */
    private long ttl;

    /** Explicit TTL flag. */
    private boolean explicitTtl;

    /**
     * Constructor.
     *
     * @param oldEntry Old entry.
     * @param newEntry New entry.
     */
    public GridDrReceiverConflictContextImpl(GridDrEntry<K, V> oldEntry, GridDrEntry<K, V> newEntry) {
        assert oldEntry != null && newEntry != null;
        assert oldEntry.ttl() >= 0 && newEntry.ttl() >= 0;

        this.oldEntry = oldEntry;
        this.newEntry = newEntry;

        // Set initial state.
        useNew();
    }

    /** {@inheritDoc} */
    @Override public GridDrEntry<K, V> oldEntry() {
        return oldEntry;
    }

    /** {@inheritDoc} */
    @Override public GridDrEntry<K, V> newEntry() {
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
        return state == State.MERGE ? S.toString(GridDrReceiverConflictContextImpl.class, this, "mergeValue", mergeVal) :
            S.toString(GridDrReceiverConflictContextImpl.class, this);
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
