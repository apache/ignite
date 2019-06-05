/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import org.h2.result.SearchRow;
import org.h2.table.TableFilter;

/**
 * A spatial index. Spatial indexes are used to speed up searching
 * spatial/geometric data.
 */
public interface SpatialIndex extends Index {

    /**
     * Find a row or a list of rows and create a cursor to iterate over the
     * result.
     *
     * @param filter the table filter (which possibly knows about additional
     *            conditions)
     * @param first the lower bound
     * @param last the upper bound
     * @param intersection the geometry which values should intersect with, or
     *            null for anything
     * @return the cursor to iterate over the results
     */
    Cursor findByGeometry(TableFilter filter, SearchRow first, SearchRow last,
            SearchRow intersection);

}
