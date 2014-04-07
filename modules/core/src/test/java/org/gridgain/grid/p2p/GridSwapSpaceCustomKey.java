/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.p2p;

import org.gridgain.grid.util.typedef.internal.*;
import java.io.*;

/**
 */
public class GridSwapSpaceCustomKey implements Serializable {
    /** */
    private long id = -1;

    /**
     * @return ID.
     */
    public long getId() {
        return id;
    }

    /**
     *
     * @param id ID.
     */
    public void setId(long id) {
        this.id = id;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        return obj instanceof GridSwapSpaceCustomKey && ((GridSwapSpaceCustomKey)obj).id == id;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Long.valueOf(id).hashCode();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridSwapSpaceCustomKey.class, this);
    }
}
