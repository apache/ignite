/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

/**
 * Wrapped value bytes of cache entry.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridCacheValueBytes {
    /** Null instance. */
    private static final GridCacheValueBytes NULL = new GridCacheValueBytes();

    /**
     * @param bytes Bytes.
     * @return Plain value bytes.
     */
    public static GridCacheValueBytes plain(Object bytes) {
        assert bytes != null && bytes instanceof byte[];

        return new GridCacheValueBytes((byte[])bytes, true);
    }

    /**
     * @param bytes Bytes.
     * @return Marshaled value bytes.
     */
    public static GridCacheValueBytes marshaled(byte[] bytes) {
        assert bytes != null;

        return new GridCacheValueBytes(bytes, false);
    }

    /**
     * @return Nil value bytes.
     */
    public static GridCacheValueBytes nil() {
        return NULL;
    }

    /** Bytes. */
    private byte[] bytes;

    /** Flag indicating if provided byte array is actual value, not marshaled data. */
    private boolean plain;

    /**
     * Private constructor for NULL instance.
     */
    public GridCacheValueBytes() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param bytes Bytes.
     * @param plain Flag indicating if provided byte array is actual value, not marshaled data.
     */
    public GridCacheValueBytes(byte[] bytes, boolean plain) {
        this.bytes = bytes;
        this.plain = plain;
    }

    /**
     * @return Bytes.
     */
    @Nullable public byte[] get() {
        return bytes;
    }

    /**
     * @return Bytes if this is plain bytes or {@code null} otherwise.
     */
    @Nullable public byte[] getIfPlain() {
        return plain && bytes != null ? bytes : null;
    }

    /**
     * @return Bytes if this is marshaled bytes or {@code null} otherwise.
     */
    @Nullable public byte[] getIfMarshaled() {
        return !plain && bytes != null ? bytes : null;
    }

    /**
     * @return Flag indicating if provided byte array is actual value, not marshaled data.
     */
    public boolean isPlain() {
        return plain;
    }

    /**
     * @return {@code True} if byte array is {@code null}.
     */
    public boolean isNull() {
        return bytes == null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheValueBytes.class, this, "len", bytes != null ? bytes.length : -1);
    }
}
