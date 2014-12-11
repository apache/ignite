/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.query;

import org.gridgain.grid.util.lang.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Convenience adapter for {@link GridQueryFieldsResult}.
 */
public class GridQueryFieldsResultAdapter implements GridQueryFieldsResult {
    /** Meta data. */
    private final List<GridQueryFieldMetadata> metaData;

    /** Result iterator. */
    private final GridCloseableIterator<List<?>> it;

    /**
     * Creates query field result composed of field metadata and iterator
     * over queried fields.
     *
     * @param metaData Meta data.
     * @param it Result iterator.
     */
    public GridQueryFieldsResultAdapter(@Nullable List<GridQueryFieldMetadata> metaData,
                                        GridCloseableIterator<List<?>> it) {
        this.metaData = metaData != null ? Collections.unmodifiableList(metaData) : null;
        this.it = it;
    }

    /** {@inheritDoc} */
    @Override public List<GridQueryFieldMetadata> metaData() {
        return metaData;
    }

    /** {@inheritDoc} */
    @Override public GridCloseableIterator<List<?>> iterator() {
        return it;
    }
}
