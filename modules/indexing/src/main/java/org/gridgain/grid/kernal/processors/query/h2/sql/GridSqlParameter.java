/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.query.h2.sql;

/**
 * Query parameter.
 */
public class GridSqlParameter extends GridSqlElement implements GridSqlValue {
    /** Index. */
    private int idx;

    /**
     * @param idx Index.
     */
    public GridSqlParameter(int idx) {
        this.idx = idx;
    }

    /** {@inheritDoc} */
    @Override public String getSQL() {
        return "?" + (idx + 1);
    }

    /**
     * @return Index.
     */
    public int index() {
        return idx;
    }

    /**
     * @param idx New index.
     */
    public void index(int idx) {
        this.idx = idx;
    }
}
