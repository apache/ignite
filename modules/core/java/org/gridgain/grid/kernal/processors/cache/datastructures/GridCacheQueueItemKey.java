/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.datastructures;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

/**
 * Queue item key.
 */
class GridCacheQueueItemKey implements Externalizable, GridCacheInternal {
    /** */
    private GridUuid uuid;

    /** */
    private String queueName;

    /** */
    private long idx;

    /**
     * Required by {@link Externalizable}.
     */
    public GridCacheQueueItemKey() {
        // No-op.
    }

    /**
     * @param uuid Queue UUID.
     * @param queueName Queue name.
     * @param idx Item index.
     */
    GridCacheQueueItemKey(GridUuid uuid, String queueName, long idx) {
        this.uuid = uuid;
        this.queueName = queueName;
        this.idx = idx;
    }

    /**
     * @return Queue UUID.
     */
    public GridUuid queueId() {
        return uuid;
    }

    /**
     * @return Item index.
     */
    public Long index() {
        return idx;
    }

    /**
     * @return Queue name.
     */
    public String queueName() {
        return queueName;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeGridUuid(out, uuid);
        U.writeString(out, queueName);
        out.writeLong(idx);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        uuid = U.readGridUuid(in);
        queueName = U.readString(in);
        idx = in.readLong();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        GridCacheQueueItemKey itemKey = (GridCacheQueueItemKey)o;

        return idx == itemKey.idx && uuid.equals(itemKey.uuid);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = uuid.hashCode();

        result = 31 * result + (int)(idx ^ (idx >>> 32));

        return result;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheQueueItemKey.class, this);
    }
}
