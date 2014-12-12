/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.query.h2.opt;


import org.apache.ignite.spi.*;
import org.h2.value.*;
import org.jetbrains.annotations.*;

/**
 * Onheap row.
 */
public class GridH2KeyValueRowOnheap extends GridH2AbstractKeyValueRow {
    /**
     * Constructor.
     *
     * @param desc Row descriptor.
     * @param key Key.
     * @param keyType Key type.
     * @param val Value.
     * @param valType Value type.
     * @param expirationTime Expiration time.
     * @throws IgniteSpiException If failed.
     */
    public GridH2KeyValueRowOnheap(GridH2RowDescriptor desc, Object key, int keyType, @Nullable Object val, int valType,
        long expirationTime) throws IgniteSpiException {
        super(desc, key, keyType, val, valType, expirationTime);
    }

    /** {@inheritDoc} */
    @Override protected void cache() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected Value getOffheapValue(int col) {
        return null;
    }
}
