/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.indexing;

import org.gridgain.grid.spi.*;

import java.util.*;

/**
 * Field query result. It is composed of
 * fields metadata and iterator over queried fields.
 * See also {@link GridIndexingSpi#queryFields(String, String, Collection, GridIndexingQueryFilter)}.
 */
public interface GridIndexingFieldsResult {
    /**
     * Gets metadata for queried fields.
     *
     * @return Meta data for queried fields.
     */
    List<GridIndexingFieldMetadata> metaData();

    /**
     * Gets iterator over queried fields.
     *
     * @return Iterator over queried fields.
     */
    GridSpiCloseableIterator<List<GridIndexingEntity<?>>> iterator();
}
