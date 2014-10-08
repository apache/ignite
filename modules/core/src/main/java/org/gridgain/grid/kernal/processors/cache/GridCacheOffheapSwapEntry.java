/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;
import sun.misc.*;

/**
 * GridCacheSwapEntry over offheap pointer.
 * <p>
 * Offheap pointer points to {@link GridCacheSwapEntryImpl} instance marshalled
 * with portable marshaller, marshaller data:
 * <ul>
 *     <li>TTL</li>
 *     <li>Expire time</li>
 *     <li>GridCacheVersion or GridCacheVersionEx</li>
 *     <li>Value is byte array flag</li>
 *     <li>Value byte array (marshalled with portable marshaller)</li>
 * </ul>
 *
 * TODO: 9189 handle big endian.
 */
public class GridCacheOffheapSwapEntry<V> implements GridCacheSwapEntry<V> {
    /** */
    private static final Unsafe UNSAFE = GridUnsafe.unsafe();

    /** */
    private static final int DATA_OFFSET = 18;

    /** */
    private final long dataStart;

    /** */
    private final long valPtr;

    /** */
    private final GridCacheVersion ver;

    /** */
    private V val;

    /** */
    private final boolean valIsByteArr;

    /**
     * @param ptr Value pointer.
     * @param size Value size.
     */
    public GridCacheOffheapSwapEntry(long ptr, int size) {
        assert ptr > 0 : ptr;
        assert size > 0 && size > DATA_OFFSET: size;
        assert UNSAFE.getByte(ptr) == 103 : UNSAFE.getByte(ptr);

        dataStart = ptr + DATA_OFFSET;

        long readPtr = dataStart + 16;

        boolean verEx = UNSAFE.getByte(readPtr++) != 0;

        ver = readVersion(readPtr, verEx);

        readPtr += verEx ? 48 : 24;

        valIsByteArr = UNSAFE.getByte(readPtr) != 0;

        assert UNSAFE.getByte(readPtr + 1) == 12 : UNSAFE.getByte(readPtr + 1); // Expect byte array.

        valPtr = readPtr;

        assert (ptr + size) == (UNSAFE.getInt(valPtr + 2) + valPtr + 6);
    }

    /**
     * @param ptr Offheap address.
     * @param verEx If {@code true} reads {@link GridCacheVersionEx} instance.
     * @return Version.
     */
    private static GridCacheVersion readVersion(long ptr, boolean verEx) {
        GridCacheVersion ver = new GridCacheVersion(UNSAFE.getInt(ptr),
            UNSAFE.getInt(ptr + 4),
            UNSAFE.getLong(ptr + 8),
            UNSAFE.getLong(ptr + 16));

        if (verEx) {
            ptr += 24;

            ver = new GridCacheVersionEx(UNSAFE.getInt(ptr),
                UNSAFE.getInt(ptr + 4),
                UNSAFE.getLong(ptr + 8),
                UNSAFE.getLong(ptr + 16),
                ver);
        }

        return ver;
    }

    /** {@inheritDoc} */
    @Override public int keyHash() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public byte[] valueBytes() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public V value() {
        return val;
    }

    /** {@inheritDoc} */
    @Override public void value(V val) {
        this.val = val;
    }

    /** {@inheritDoc} */
    @Override public boolean valueIsByteArray() {
        return valIsByteArr;
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion version() {
        return ver;
    }

    /** {@inheritDoc} */
    @Override public long ttl() {
        return UNSAFE.getLong(dataStart);
    }

    /** {@inheritDoc} */
    @Override public long expireTime() {
        return UNSAFE.getLong(dataStart + 8);
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridUuid keyClassLoaderId() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public long offheapPointer() {
        return valPtr;
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridUuid valueClassLoaderId() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheOffheapSwapEntry.class, this);
    }
}
