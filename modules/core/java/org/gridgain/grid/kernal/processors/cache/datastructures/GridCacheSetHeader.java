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
 * Cache set header.
 */
public class GridCacheSetHeader implements GridCacheInternal, Externalizable {
    /** */
    private GridUuid uuid;

    /**
     * Required by {@link Externalizable}.
     */
    public GridCacheSetHeader() {
        // No-op.
    }

    /**
     * @return Set UUID.
     */
    public GridUuid setId() {
        return uuid;
    }

    /**
     * @param uuid Set UUID.
     */
    public GridCacheSetHeader(GridUuid uuid) {
        this.uuid = uuid;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeGridUuid(out, uuid);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        uuid = U.readGridUuid(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheSetHeader.class, this);
    }
}
