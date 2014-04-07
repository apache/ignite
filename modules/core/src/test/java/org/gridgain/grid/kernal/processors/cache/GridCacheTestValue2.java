/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.util.typedef.internal.*;
import java.io.*;

/**
 * Second test value.
 */
public class GridCacheTestValue2 implements Serializable {
    /** */
    @GridCacheQuerySqlField
    private String val;

    /**
     *
     */
    public GridCacheTestValue2() {
        /* No-op. */
    }

    /**
     *
     * @param val Value.
     */
    public GridCacheTestValue2(String val) {
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
        return S.toString(GridCacheTestValue2.class, this);
    }
}