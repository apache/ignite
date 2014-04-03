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
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

/**
 * Set item key.
 */
class GridCacheSetItemKey implements GridCacheInternal, Externalizable {
    /** */
    private String setName;

    /** */
    private GridUuid uuid;

    /** */
    @GridToStringInclude
    private Object item;

    /**
     * Required by {@link java.io.Externalizable}.
     */
    public GridCacheSetItemKey() {
        // No-op.
    }

    /**
     * @param setName Set name.
     * @param uuid Set UUID.
     * @param item Set item.
     */
    GridCacheSetItemKey(String setName, GridUuid uuid, Object item) {
        this.setName = setName;
        this.uuid = uuid;
        this.item = item;
    }

    /**
     * @return Set name.
     */
    public String setName() {
        return setName;
    }

    /**
     * @return Set UUID.
     */
    public GridUuid setId() {
        return uuid;
    }

    /**
     * @return Set item.
     */
    public Object item() {
        return item;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = uuid.hashCode();

        result = 31 * result + item.hashCode();

        return result;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        GridCacheSetItemKey that = (GridCacheSetItemKey)o;

        return uuid.equals(that.uuid) && item.equals(that.item);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeString(out, setName);
        U.writeGridUuid(out, uuid);
        out.writeObject(item);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        setName = U.readString(in);
        uuid = U.readGridUuid(in);
        item = in.readObject();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheSetItemKey.class, this);
    }
}
