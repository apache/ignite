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
    private GridUuid setId;

    /** */
    @GridToStringInclude
    private Object item;

    /**
     * Required by {@link Externalizable}.
     */
    public GridCacheSetItemKey() {
        // No-op.
    }

    /**
     * @param setName Set name.
     * @param setId Set unique ID.
     * @param item Set item.
     */
    GridCacheSetItemKey(String setName, GridUuid setId, Object item) {
        this.setName = setName;
        this.setId = setId;
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
        return setId;
    }

    /**
     * @return Set item.
     */
    public Object item() {
        return item;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = setId.hashCode();

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

        return setId.equals(that.setId) && item.equals(that.item);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeString(out, setName);
        U.writeGridUuid(out, setId);
        out.writeObject(item);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        setName = U.readString(in);
        setId = U.readGridUuid(in);
        item = in.readObject();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheSetItemKey.class, this);
    }
}
