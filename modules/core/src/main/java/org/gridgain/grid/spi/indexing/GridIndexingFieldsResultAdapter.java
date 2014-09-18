/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.indexing;

import org.gridgain.grid.spi.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Convenience adapter for {@link GridIndexingFieldsResult}.
 */
public class GridIndexingFieldsResultAdapter implements GridIndexingFieldsResult {
    /** Meta data. */
    private final List<GridIndexingFieldMetadata> metaData;

    /** Result iterator. */
    private final GridSpiCloseableIterator<List<GridIndexingEntity<?>>> it;

    /**
     * Creates query field result composed of field metadata and iterator
     * over queried fields.
     *
     * @param metaData Meta data.
     * @param it Result iterator.
     */
    public GridIndexingFieldsResultAdapter(@Nullable List<GridIndexingFieldMetadata> metaData,
        GridSpiCloseableIterator<List<GridIndexingEntity<?>>> it) {
        this.metaData = metaData != null ? Collections.unmodifiableList(metaData) : null;
        this.it = it;
    }

    /** {@inheritDoc} */
    @Override public List<GridIndexingFieldMetadata> metaData() {
        return metaData;
    }

    /** {@inheritDoc} */
    @Override public GridSpiCloseableIterator<List<GridIndexingEntity<?>>> iterator() {
        return it;
    }
}
