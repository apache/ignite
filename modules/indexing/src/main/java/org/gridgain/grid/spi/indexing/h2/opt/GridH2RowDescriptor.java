/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.indexing.h2.opt;

import org.apache.ignite.spi.*;
import org.gridgain.grid.*;
import org.gridgain.grid.spi.indexing.h2.*;
import org.gridgain.grid.util.offheap.unsafe.*;
import org.jetbrains.annotations.*;

/**
 * Row descriptor.
 */
public interface GridH2RowDescriptor extends GridOffHeapSmartPointerFactory<GridH2KeyValueRowOffheap> {
    /**
     * @return SPI.
     */
    public GridH2IndexingSpi spi();

    /**
     * Creates new row.
     *
     * @param key Key.
     * @param val Value.
     * @param expirationTime Expiration time in millis.
     * @return Row.
     * @throws org.apache.ignite.spi.IgniteSpiException If failed.
     */
    public GridH2AbstractKeyValueRow createRow(Object key, @Nullable Object val, long expirationTime)
        throws IgniteSpiException;

    /**
     * @param key Cache key.
     * @return Value.
     * @throws GridException If failed.
     */
    public Object readFromSwap(Object key) throws GridException;

    /**
     * @return Value type.
     */
    public int valueType();

    /**
     * @return {@code true} If we need to store {@code toString()} of value.
     */
    public boolean valueToString();

    /**
     * @return Total fields count.
     */
    public int fieldsCount();

    /**
     * Gets value type for column index.
     *
     * @param col Column index.
     * @return Value type.
     */
    public int fieldType(int col);

    /**
     * Gets column value by column index.
     *
     * @param obj Object to extract value from.
     * @param col Column index.
     * @return  Column value.
     */
    public Object columnValue(Object obj, int col);

    /**
     * @param col Column index.
     * @return {@code True} if column relates to key, false if it relates to value.
     */
    public boolean isKeyColumn(int col);

    /**
     * @return Unsafe memory.
     */
    public GridUnsafeMemory memory();

    /**
     * @param row Deserialized offheap row to cache in heap.
     */
    public void cache(GridH2KeyValueRowOffheap row);

    /**
     * @param ptr Offheap pointer to remove from cache.
     */
    public void uncache(long ptr);

    /**
     * @return Guard.
     */
    public GridUnsafeGuard guard();
}
