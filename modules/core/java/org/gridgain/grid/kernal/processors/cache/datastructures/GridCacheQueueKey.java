/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.datastructures;

import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

/**
 * TODO
 */
public class GridCacheQueueKey implements Externalizable, GridCacheInternal {
    /** */
    private String name;

    public GridCacheQueueKey() {
    }

    public GridCacheQueueKey(String name) {
        this.name = name;
    }

    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeString(out, name);
    }

    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        name = U.readString(in);
    }

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        GridCacheQueueKey queueKey = (GridCacheQueueKey)o;

        return name.equals(queueKey.name);
    }

    @Override public int hashCode() {
        return name.hashCode();
    }
}
