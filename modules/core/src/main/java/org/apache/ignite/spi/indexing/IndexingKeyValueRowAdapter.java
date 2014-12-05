/* @java.file.header */

/*  _________        _____ __________________        _____
*  __  ____/___________(_)______  /__  ____/______ ____(_)_______
*  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
*  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
*  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
*/

package org.apache.ignite.spi.indexing;

import org.gridgain.grid.kernal.processors.cache.query.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;
import org.jetbrains.annotations.*;

/**
 * Convenience adapter for {@link IndexingKeyValueRow}.
 */
public class IndexingKeyValueRowAdapter<K, V> implements IndexingKeyValueRow<K, V> {
    /** Key. */
    @GridToStringInclude
    private IndexingEntity<K> key;

    /** Value. */
    @GridToStringInclude
    private IndexingEntity<V> val;

    /** Version. */
    @GridToStringInclude
    private byte[] ver;

    /**
     * Constructor.
     *
     * @param key Key.
     * @param val Value.
     */
    public IndexingKeyValueRowAdapter(K key, V val) {
        assert key != null;
        assert val != null;

        this.key = new IndexingEntityAdapter<>(key, null);
        this.val = new IndexingEntityAdapter<>(val, null);
    }

    /**
     * Constructs query index row.
     *
     * @param key Key.
     * @param val Value.
     * @param ver Version. It is {@code null} in case of {@link GridCacheQueryType#SCAN} query.
     */
    public IndexingKeyValueRowAdapter(IndexingEntity<K> key, @Nullable IndexingEntity<V> val,
                                      @Nullable byte[] ver) {
        assert key != null;

        this.key = key;
        this.val = val;
        this.ver = ver;
    }

    /** {@inheritDoc} */
    @Override public IndexingEntity<K> key() {
        return key;
    }

    /** {@inheritDoc} */
    @Override public IndexingEntity<V> value() {
        return val;
    }

    /** {@inheritDoc} */
    @Override public byte[] version() {
        return ver;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IndexingKeyValueRowAdapter.class, this);
    }
}
