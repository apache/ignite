// @java.file.header

/*  _________        _____ __________________        _____
*  __  ____/___________(_)______  /__  ____/______ ____(_)_______
*  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
*  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
*  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
*/

package org.gridgain.grid.spi.indexing;

import org.gridgain.grid.kernal.processors.cache.query.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;
import org.jetbrains.annotations.*;

/**
 * Convenience adapter for {@link GridIndexingKeyValueRow}.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridIndexingKeyValueRowAdapter<K, V> implements GridIndexingKeyValueRow<K, V> {
    /** Key. */
    @GridToStringInclude
    private GridIndexingEntity<K> key;

    /** Value. */
    @GridToStringInclude
    private GridIndexingEntity<V> val;

    /** Version. */
    @GridToStringInclude
    private byte[] ver;

    /**
     * Constructor.
     *
     * @param key Key.
     * @param val Value.
     */
    public GridIndexingKeyValueRowAdapter(K key, V val) {
        assert key != null;
        assert val != null;

        this.key = new GridIndexingEntityAdapter<>(key, null);
        this.val = new GridIndexingEntityAdapter<>(val, null);
    }

    /**
     * Constructs query index row.
     *
     * @param key Key.
     * @param val Value.
     * @param ver Version. It is {@code null} in case of {@link GridCacheQueryType#SCAN} query.
     */
    public GridIndexingKeyValueRowAdapter(GridIndexingEntity<K> key, @Nullable GridIndexingEntity<V> val,
        @Nullable byte[] ver) {
        assert key != null;

        this.key = key;
        this.val = val;
        this.ver = ver;
    }

    /** {@inheritDoc} */
    @Override public GridIndexingEntity<K> key() {
        return key;
    }

    /** {@inheritDoc} */
    @Override public GridIndexingEntity<V> value() {
        return val;
    }

    /** {@inheritDoc} */
    @Override public byte[] version() {
        return ver;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridIndexingKeyValueRowAdapter.class, this);
    }
}
