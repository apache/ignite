/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi.indexing;

import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;
import org.jetbrains.annotations.*;

/**
 * Convenience adapter for {@link IndexingEntity}.
 */
public class IndexingEntityAdapter<T> implements IndexingEntity<T> {
    /** */
    @GridToStringInclude
    private final T val;

    /** */
    @GridToStringExclude
    private final byte[] bytes;

    /**
     * @param val Value.
     * @param bytes Value marshalled by {@link org.apache.ignite.marshaller.IgniteMarshaller}.
     */
    public IndexingEntityAdapter(T val, @Nullable byte[] bytes) {
        this.val = val;
        this.bytes = bytes;
    }

    /** {@inheritDoc} */
    @Override public T value() {
        return val;
    }

    /** {@inheritDoc} */
    @Override public byte[] bytes() {
        return bytes;
    }

    /** {@inheritDoc} */
    @Override public boolean hasValue() {
        return val != null || (val == null && bytes == null);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IndexingEntityAdapter.class, this,
            "bytesLength", (bytes == null ? 0 : bytes.length));
    }
}
