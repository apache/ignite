/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;

import java.io.*;

/**
 * Internal key that is guaranteed to be mapped on particular partition.
 * This class is used for group-locking transactions that lock the whole partition.
 */
public class GridPartitionLockKey implements GridCacheInternal, Externalizable {
    /** Partition ID. */
    private int partId;

    /** Key itself. */
    @GridToStringInclude
    private Object key;

    /**
     * Required by {@link Externalizable}.
     */
    public GridPartitionLockKey() {
        // No-op.
    }

    /**
     * @param key Key.
     */
    public GridPartitionLockKey(Object key) {
        assert key != null;

        this.key = key;
    }

    /**
     * @param partId Partition ID.
     */
    public void partitionId(int partId) {
        this.partId = partId;
    }

    /**
     * @return Partition ID.
     */
    public int partitionId() {
        return partId;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (! (o instanceof GridPartitionLockKey))
            return false;

        GridPartitionLockKey that = (GridPartitionLockKey)o;

        return key.equals(that.key);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return key.hashCode();
    }

    /** {@inheritDoc} */
    public String toString() {
        return S.toString(GridPartitionLockKey.class, this);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(partId);
        out.writeObject(key);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        partId = in.readInt();
        key = in.readObject();
    }
}
