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
 * Test value.
 */
public class GridCacheTestValue implements Serializable, Cloneable {
    /** */
    @GridCacheQuerySqlField(unique = true)
    private String val;

    /**
     *
     */
    public GridCacheTestValue() {
        /* No-op. */
    }

    /**
     *
     * @param val Value.
     */
    public GridCacheTestValue(String val) {
        this.val = val;
    }

    /**
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
    @Override protected Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        return this == o || !(o == null || getClass() != o.getClass())
            && val != null && val.equals(((GridCacheTestValue)o).val);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheTestValue.class, this);
    }
}