/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.indexing;

import org.apache.ignite.spi.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Convenience adapter for {@link IndexingFieldsResult}.
 */
public class IndexingFieldsResultAdapter implements IndexingFieldsResult {
    /** Meta data. */
    private final List<IndexingFieldMetadata> metaData;

    /** Result iterator. */
    private final IgniteSpiCloseableIterator<List<IndexingEntity<?>>> it;

    /**
     * Creates query field result composed of field metadata and iterator
     * over queried fields.
     *
     * @param metaData Meta data.
     * @param it Result iterator.
     */
    public IndexingFieldsResultAdapter(@Nullable List<IndexingFieldMetadata> metaData,
                                       IgniteSpiCloseableIterator<List<IndexingEntity<?>>> it) {
        this.metaData = metaData != null ? Collections.unmodifiableList(metaData) : null;
        this.it = it;
    }

    /** {@inheritDoc} */
    @Override public List<IndexingFieldMetadata> metaData() {
        return metaData;
    }

    /** {@inheritDoc} */
    @Override public IgniteSpiCloseableIterator<List<IndexingEntity<?>>> iterator() {
        return it;
    }
}
