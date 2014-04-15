/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.util.typedef.internal.*;

/**
 * Thrown when an operation is performed on removed entry.
 */
public class GridCacheFilterFailedException extends Exception {
    /** */
    private static final long serialVersionUID = 0L;

    /** Value for which filter failed. */
    private final Object val;

    /**
     * Empty constructor.
     */
    public GridCacheFilterFailedException() {
        val = null;
    }

    /**
     * @param val Value for which filter failed.
     */
    public GridCacheFilterFailedException(Object val) {
        this.val = val;
    }

    /**
     * @return Value for failed filter.
     */
    @SuppressWarnings({"unchecked"})
    public <V> V value() {
        return (V)val;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheFilterFailedException.class, this);
    }
}
