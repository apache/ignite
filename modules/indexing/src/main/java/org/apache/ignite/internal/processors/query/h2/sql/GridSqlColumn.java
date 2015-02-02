/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.processors.query.h2.sql;

/**
 * Column.
 */
public class GridSqlColumn extends GridSqlElement implements GridSqlValue {
    /** */
    private final GridSqlElement expressionInFrom;

    /** */
    private final String colName;

    /** SQL from original query. May be qualified or unqualified column name. */
    private final String sqlText;

    /**
     * @param from From.
     * @param name Name.
     * @param sqlText Text.
     */
    public GridSqlColumn(GridSqlElement from, String name, String sqlText) {
        assert sqlText != null;

        expressionInFrom = from;
        colName = name;
        this.sqlText = sqlText;
    }

    /**
     * @return Simple unqualified column with only name.
     */
    public GridSqlColumn simplify() {
        return new GridSqlColumn(null, colName, colName);
    }

    /**
     * @return Column name.
     */
    public String columnName() {
        return colName;
    }

    /** {@inheritDoc} */
    @Override public String getSQL() {
        return sqlText;
    }

    /**
     * @return Expression in from.
     */
    public GridSqlElement expressionInFrom() {
        return expressionInFrom;
    }
}
