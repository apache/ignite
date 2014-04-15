/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.ggfs;

import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

/**
 * Internal key used to track if sampling enabled or disabled for particular GGFS instance.
 */
class GridGgfsSamplingKey implements GridCacheInternal, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** GGFS name. */
    private String name;

    /**
     * Default constructor.
     *
     * @param name - GGFS name.
     */
    GridGgfsSamplingKey(String name) {
        this.name = name;
    }

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridGgfsSamplingKey() {
        // No-op.
    }

    /**
     * @return GGFS name.
     */
    public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return name == null ? 0 : name.hashCode();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        return this == obj || (obj instanceof GridGgfsSamplingKey && F.eq(name, ((GridGgfsSamplingKey)obj).name));
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeString(out, name);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException {
        name = U.readString(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridGgfsSamplingKey.class, this);
    }
}
