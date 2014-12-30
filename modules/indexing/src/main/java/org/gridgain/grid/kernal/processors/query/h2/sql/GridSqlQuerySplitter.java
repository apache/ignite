/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.query.h2.sql;

import org.gridgain.grid.kernal.processors.cache.query.*;

import java.sql.*;
import java.util.*;

/**
 * Splits a single SQL query into two step map-reduce query.
 */
public class GridSqlQuerySplitter {
    /**
     * @param conn Connection.
     * @param query Query.
     * @param params Parameters.
     * @return Two step query.
     */
    public GridCacheTwoStepQuery split(Connection conn, String query, Collection<?> params) {
        GridSqlSelect qry = GridSqlQueryParser.parse(conn, query);

//        GridSqlSelect rdcQry = qry.clone();

        for (GridSqlElement el : qry.select()) {

        }

        if (qry.distinct()) {

        }

        qry.from();

        qry.where();

        qry.groups();

        qry.having();

        qry.sort();
    }

    private boolean checkGroup(GridSqlSelect qry) {
        if (qry.distinct())
            return true;

        qry.from();

        qry.where();

        qry.groups();

        qry.having();

        qry.sort();
    }
}
