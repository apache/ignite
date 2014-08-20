/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.service;

import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.typedef.internal.*;

/**
 * Service configuration key.
 */
public class GridServiceAssignmentsKey extends GridCacheUtilityKey<GridServiceAssignmentsKey> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Service name. */
    private final String name;

    /**
     * @param name Service ID.
     */
    public GridServiceAssignmentsKey(String name) {
        assert name != null;

        this.name = name;
    }

    /**
     * @return Service name.
     */
    public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override protected boolean equalsx(GridServiceAssignmentsKey that) {
        return name.equals(that.name);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return name.hashCode();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridServiceAssignmentsKey.class, this);
    }
}
