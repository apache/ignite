/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.query.h2.sql;

import org.h2.util.*;

/**
 *
 */
public class GridJoin extends GridSqlElement {

    /** */
    private final GridSqlElement tbl1;

    /** */
    private final GridSqlElement tbl2;

    /**
     * @param tbl1 Table 1.
     * @param tbl2 Table 2.
     */
    public GridJoin(GridSqlElement tbl1, GridSqlElement tbl2) {
        this.tbl1 = tbl1;
        this.tbl2 = tbl2;
    }

    /**
     * @return Table 1.
     */
    public GridSqlElement table1() {
        return tbl1;
    }

    /**
     * @return Table 2.
     */
    public GridSqlElement table2() {
        return tbl2;
    }

    /** {@inheritDoc} */
    @Override public String getSQL() {
        StatementBuilder buff = new StatementBuilder();

        buff.append(tbl1.getSQL());

        buff.append(" \n INNER JOIN ");

        buff.append(tbl2.getSQL());

        return buff.toString();
    }
}
