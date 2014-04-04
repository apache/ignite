/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.util.typedef.internal.*;
import java.io.*;

/**
 * Test key.
 */
public class GridCacheTestKey implements Serializable {
    /** */
    private String val;

    /**
     *
     */
    public GridCacheTestKey() {
        /* No-op. */
    }

    /**
     * @param val Value.
     */
    public GridCacheTestKey(String val) {
        this.val = val;
    }

    /**
     *
     * @return Value.
     */
    public String getValue() {
        return val;
    }

    /**
     *
     * @param val Value.
     */
    public void setValue(String val) {
        this.val = val;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheTestKey.class, this);
    }
}