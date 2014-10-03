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
 * GridCacheSwapEntry over offheap pointer (points to {@link GridCacheSwapEntryImpl}
 * with portable marshaller).
 * <p>
 * Offheap pointer points to {@link GridCacheSwapEntryImpl} instance marshalled
 * with portable marshaller, marshaller data:
 * <ul>
 *     <li>TTL</li>
 *     <li>Expire time</li>
 *     <li>Cache version or </li>
 * </ul>
 *
 * TODO: 9189 handle big endian.
 */
public class GridCacheOffheapSwapEntry implements GridCacheSwapEntry {
    /** */
    private static final Unsafe UNSAFE = GridUnsafe.unsafe();

    /** */
    private static final int DATA_OFFSET = 18;

    /** */
    private final GridBiTuple<Long, Integer> ptr;

    /** */
    private final long start;

    /** */
    private Object val;

    /**
     * @param ptr Tuple where first value is pointer and second is value size.
     */
    public GridCacheOffheapSwapEntry(GridBiTuple<Long, Integer> ptr) {
        assert ptr != null;
        assert ptr.get1() != null && ptr.get1() > 0 : ptr.get1();
        assert ptr.get2() != null && ptr.get2() > DATA_OFFSET : ptr.get2();

        this.ptr = ptr;

        long start0 = ptr.get1();

        assert UNSAFE.getByte(start0) == 103 : UNSAFE.getByte(start0);

        start = start0 + DATA_OFFSET;

        /**
         raw.writeLong(ttl);
         raw.writeLong(expireTime);

         raw.writeInt(keyHash);

         writeVersion(raw, ver, true);

         raw.writeBoolean(valIsByteArr);
         raw.writeByteArray(valBytes);
         */
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
    @Override public Object value() {
        return val;
    }

    @Override public void value(Object val) {
        this.val = val;
    }

    @Override public boolean valueIsByteArray() {
        return false;
    }

    @Override public GridCacheVersion version() {
        return null;
    }

    @Override public long ttl() {
        return 0;
    }

    @Override public long expireTime() {
        return 0;
    }

    @Nullable @Override public GridUuid keyClassLoaderId() {
        throw new UnsupportedOperationException();
    }

    @Nullable @Override public GridBiTuple<Long, Integer> offheapPointer() {
        return null;
    }

    @Nullable @Override public GridUuid valueClassLoaderId() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheOffheapSwapEntry.class, this);
    }
}
