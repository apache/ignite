/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.loadtests.h2indexing;

import org.gridgain.grid.cache.query.*;

import java.util.*;

/**
 * Test entity.
 */
public class GridTestEntity {
    /** */
    @GridCacheQuerySqlField(index = true)
    private final String name;

    /** */
    @GridCacheQuerySqlField(index = false)
    private final Date date;

    /**
     * Constructor.
     *
     * @param name Name.
     * @param date Date.
     */
    @SuppressWarnings("AssignmentToDateFieldFromParameter")
    public GridTestEntity(String name, Date date) {
        this.name = name;
        this.date = date;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GridTestEntity that = (GridTestEntity) o;

        return !(date != null ? !date.equals(that.date) : that.date != null) &&
            !(name != null ? !name.equals(that.name) : that.name != null);

    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = name != null ? name.hashCode() : 0;

        res = 31 * res + (date != null ? date.hashCode() : 0);

        return res;
    }
}
