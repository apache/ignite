/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.offheap;

import java.util.*;

/**
 * Array wrapper for correct equals and hash code.
 */
public class GridByteArrayWrapper {
    /** Wrapped array. */
    private byte[] arr;

    /**
     * Constructor.
     *
     * @param arr Wrapped array.
     */
    public GridByteArrayWrapper(byte[] arr) {
        this.arr = arr;
    }

    /**
     * @return Wrapped byte array.
     */
    public byte[] array() {
        return arr;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof GridByteArrayWrapper))
            return false;

        GridByteArrayWrapper that = (GridByteArrayWrapper)o;

        return Arrays.equals(arr, that.arr);

    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Arrays.hashCode(arr);
    }
}
