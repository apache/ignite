/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.android;

import java.util.Map;
import java.util.Set;
import org.h2.util.StringUtils;
import android.database.Cursor;

/**
 * This helper class is used to build SQL statements.
 */
@SuppressWarnings("unused")
public class H2QueryBuilder {

    private H2Database.CursorFactory factory;
    private boolean distinct;
    private String tables;
    private Map<String, String> columnMap;

    /**
     * Append the column to the string builder. The columns are separated by
     * comma.
     *
     * @param s the target string builder
     * @param columns the columns
     */
    static void appendColumns(StringBuilder s, String[] columns) {
        for (int i = 0; i < columns.length; i++) {
            if (i > 0) {
                s.append(", ");
            }
            s.append(StringUtils.quoteIdentifier(columns[i]));
        }
    }

    /**
     * Return the SELECT statement for the given parameters.
     *
     * @param distinct if only distinct rows should be returned
     * @param tables the list of tables
     * @param columns the list of columns
     * @param where the where condition or null
     * @param groupBy the group by list or null
     * @param having the having condition or null
     * @param orderBy the order by list or null
     * @param limit the limit or null
     * @return the query
     */
    static String buildQueryString(boolean distinct, String tables,
            String[] columns, String where, String groupBy, String having,
            String orderBy, String limit) {
        StringBuilder s = new StringBuilder();
        s.append("select ");
        if (distinct) {
            s.append("distinct ");
        }
        appendColumns(s, columns);
        s.append(" from ").append(tables);
        if (where != null) {
            s.append(" where ").append(where);
        }
        if (groupBy != null) {
            s.append(" group by ").append(groupBy);
        }
        if (having != null) {
            s.append(" having ").append(having);
        }
        if (orderBy != null) {
            s.append(" order by ").append(groupBy);
        }
        if (limit != null) {
            s.append(" limit ").append(limit);
        }
        return s.toString();
    }

    /**
     * Append the text to the where clause.
     *
     * @param inWhere the text to append
     */
    void appendWhere(CharSequence inWhere) {
        // TODO
    }

    /**
     * Append the text to the where clause. The text is escaped.
     *
     * @param inWhere the text to append
     */
    void appendWhereEscapeString(String inWhere) {
        // TODO how to escape
    }

    /**
     * Return the SELECT UNION statement for the given parameters.
     *
     * @param projectionIn TODO
     * @param selection TODO
     * @param selectionArgs TODO
     * @param groupBy the group by list or null
     * @param having the having condition or null
     * @param orderBy the order by list or null
     * @param limit the limit or null
     * @return the query
     */
    String buildQuery(String[] projectionIn, String selection,
            String[] selectionArgs, String groupBy, String having,
            String orderBy, String limit) {
        return null;
    }

    /**
     * Return the SELECT UNION statement for the given parameters.
     *
     * @param subQueries TODO
     * @param orderBy the order by list or null
     * @param limit the limit or null
     * @return the query
     */
    String buildUnionQuery(String[] subQueries, String orderBy, String limit) {
        return null;

    }

    /**
     * Return the SELECT UNION statement for the given parameters.
     *
     * @param typeDiscriminatorColumn TODO
     * @param unionColumns TODO
     * @param columnsPresentInTable TODO
     * @param computedColumnsOffset TODO
     * @param typeDiscriminatorValue TODO
     * @param selection TODO
     * @param selectionArgs TODO
     * @param groupBy the group by list or null
     * @param having the having condition or null
     * @return the query
     */
    String buildUnionSubQuery(String typeDiscriminatorColumn,
            String[] unionColumns, Set<String> columnsPresentInTable,
            int computedColumnsOffset, String typeDiscriminatorValue,
            String selection, String[] selectionArgs, String groupBy,
            String having) {
        return null;

    }

    /**
     * Get the list of tables.
     *
     * @return the list of tables
     */
    String getTables() {
        return tables;
    }

    /**
     * Run the query for the given parameters.
     *
     * @param db the connection
     * @param projectionIn TODO
     * @param selection TODO
     * @param selectionArgs TODO
     * @param groupBy the group by list or null
     * @param having the having condition or null
     * @param orderBy the order by list or null
     * @return the cursor
     */
    Cursor query(H2Database db, String[] projectionIn, String selection,
            String[] selectionArgs, String groupBy, String having,
            String orderBy) {
        return null;
    }

    /**
     * Run the query for the given parameters.
     *
     * @param db the connection
     * @param projectionIn TODO
     * @param selection TODO
     * @param selectionArgs TODO
     * @param groupBy the group by list or null
     * @param having the having condition or null
     * @param orderBy the order by list or null
     * @param limit the limit or null
     * @return the cursor
     */
    Cursor query(H2Database db, String[] projectionIn, String selection,
            String[] selectionArgs, String groupBy, String having,
            String orderBy, String limit) {
        return null;
    }

    /**
     * Set the cursor factory.
     *
     * @param factory the new value
     */
    void setCursorFactory(H2Database.CursorFactory factory) {
        this.factory = factory;
    }

    /**
     * Enable or disable the DISTINCT flag.
     *
     * @param distinct the new value
     */
    void setDistinct(boolean distinct) {
        this.distinct = distinct;
    }

    /**
     * TODO
     *
     * @param columnMap TODO
     */
    void setProjectionMap(Map<String, String> columnMap) {
        this.columnMap = columnMap;
    }

    /**
     * Set the list of tables.
     *
     * @param inTables the list of tables
     */
    void setTables(String inTables) {
        this.tables = inTables;
    }

}
