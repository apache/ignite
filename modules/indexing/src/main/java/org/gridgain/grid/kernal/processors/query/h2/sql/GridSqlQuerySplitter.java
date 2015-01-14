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
    /** */
    private static final String TABLE_PREFIX = "__T";

    /** */
    private static final String COLUMN_PREFIX = "__C";

    /**
     * @param idx Index of table.
     * @return Table name.
     */
    private static String table(int idx) {
        return TABLE_PREFIX + idx;
    }

    /**
     * @param idx Index of column.
     * @return Column alias.
     */
    private static String column(int idx) {
        return COLUMN_PREFIX + idx;
    }

    /**
     * @param conn Connection.
     * @param query Query.
     * @param params Parameters.
     * @return Two step query.
     */
    public static GridCacheTwoStepQuery split(Connection conn, String query, Object[] params) {
        GridSqlSelect srcQry = GridSqlQueryParser.parse(conn, query);

        if (srcQry.groups().isEmpty()) { // Simple case.
            String tbl0 = table(0);

            GridCacheTwoStepQuery res = new GridCacheTwoStepQuery("select * from " + tbl0);

            res.addMapQuery(tbl0, srcQry.getSQL(), params);

            return res;
        }

        // Map query.
        GridSqlSelect mapQry = srcQry.clone();

        mapQry.clearSelect();

        List<GridSqlAlias> aliases = new ArrayList<>(srcQry.allExpressions().size());

        int idx = 0;

        for (GridSqlElement exp : srcQry.allExpressions()) { // Add all expressions to select clause.
            if (exp instanceof GridSqlColumn)
                exp = new GridSqlAlias(((GridSqlColumn)exp).columnName(), exp);
            else if (!(exp instanceof GridSqlAlias))
                exp = new GridSqlAlias(column(idx), exp);

            aliases.add((GridSqlAlias)exp);

            mapQry.addSelectExpression(exp);

            idx++;

            assert aliases.size() == idx;
        }

        mapQry.clearGroups();

        for (int col : srcQry.groupColumns())
            mapQry.addGroupExpression(new GridSqlColumn(null, null, aliases.get(col).alias()));

        mapQry.clearSort(); // TODO sort support

        // Reduce query.
        GridSqlSelect rdcQry = new GridSqlSelect();

        for (int i = 0; i < srcQry.select().size(); i++)
            rdcQry.addSelectExpression(new GridSqlColumn(null, null, aliases.get(i).alias()));

        rdcQry.from(new GridSqlTable(null, table(0)));

        for (int col : srcQry.groupColumns())
            rdcQry.addGroupExpression(new GridSqlColumn(null, null, aliases.get(col).alias()));

        GridCacheTwoStepQuery res = new GridCacheTwoStepQuery(rdcQry.getSQL());

        res.addMapQuery(table(0), mapQry.getSQL(), params);

        return res;
    }
}
