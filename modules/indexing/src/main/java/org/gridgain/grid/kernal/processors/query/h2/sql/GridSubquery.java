/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.query.h2.sql;

/**
 * Subquery.
 */
public class GridSubquery extends GridSqlElement {
    /** */
    private GridSelect select;

    /**
     * @param select Select.
     */
    public GridSubquery(GridSelect select) {
        this.select = select;
    }

    /** {@inheritDoc} */
    @Override public String getSQL() {
        return "(" + select.getSQL() + ")";
    }

    /**
     * @return Select.
     */
    public GridSelect select() {
        return select;
    }

    /**
     * @param select New select.
     */
    public void select(GridSelect select) {
        this.select = select;
    }
}
