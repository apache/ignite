/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.processors.query.h2.sql;

import org.h2.util.*;
import org.jetbrains.annotations.*;

/**
 * Join of two tables or subqueries.
 */
public class GridSqlJoin extends GridSqlElement {
    /**
     * @param leftTbl Left table.
     * @param rightTbl Right table.
     */
    public GridSqlJoin(GridSqlElement leftTbl, GridSqlElement rightTbl) {
        addChild(leftTbl);
        addChild(rightTbl);
    }

    /**
     * @return Table 1.
     */
    public GridSqlElement leftTable() {
        return child(0);
    }

    /**
     * @return Table 2.
     */
    public GridSqlElement rightTable() {
        return child(1);
    }

    /**
     * @return {@code ON} Condition.
     */
    @Nullable public GridSqlElement on() {
        return child(2);
    }

    /**
     * @return {@code true} If it is a {@code LEFT JOIN}.
     */
    public boolean leftJoin() {
        return false; // TODO
    }

    /** {@inheritDoc} */
    @Override public String getSQL() {
        StatementBuilder buff = new StatementBuilder();

        buff.append(leftTable().getSQL());

        buff.append(leftJoin() ? " \n LEFT JOIN " : " \n INNER JOIN ");

        buff.append(rightTable().getSQL());

        return buff.toString();
    }
}
