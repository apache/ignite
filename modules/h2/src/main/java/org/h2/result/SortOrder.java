/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import org.h2.command.dml.SelectOrderBy;
import org.h2.engine.Database;
import org.h2.engine.SysProperties;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.table.Column;
import org.h2.table.TableFilter;
import org.h2.util.StatementBuilder;
import org.h2.util.StringUtils;
import org.h2.util.Utils;
import org.h2.value.Value;
import org.h2.value.ValueNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

/**
 * A sort order represents an ORDER BY clause in a query.
 */
public class SortOrder implements Comparator<Value[]> {

    /**
     * This bit mask means the values should be sorted in ascending order.
     */
    public static final int ASCENDING = 0;

    /**
     * This bit mask means the values should be sorted in descending order.
     */
    public static final int DESCENDING = 1;

    /**
     * This bit mask means NULLs should be sorted before other data, no matter
     * if ascending or descending order is used.
     */
    public static final int NULLS_FIRST = 2;

    /**
     * This bit mask means NULLs should be sorted after other data, no matter
     * if ascending or descending order is used.
     */
    public static final int NULLS_LAST = 4;

    /**
     * The default sort order for NULL.
     */
    private static final int DEFAULT_NULL_SORT =
            SysProperties.SORT_NULLS_HIGH ? 1 : -1;

    /**
     * The default sort order bit for NULLs last.
     */
    private static final int DEFAULT_NULLS_LAST = SysProperties.SORT_NULLS_HIGH ? NULLS_LAST : NULLS_FIRST;

    /**
     * The default sort order bit for NULLs first.
     */
    private static final int DEFAULT_NULLS_FIRST = SysProperties.SORT_NULLS_HIGH ? NULLS_FIRST : NULLS_LAST;

    private final Database database;

    /**
     * The column indexes of the order by expressions within the query.
     */
    private final int[] queryColumnIndexes;

    /**
     * The sort type bit mask (DESCENDING, NULLS_FIRST, NULLS_LAST).
     */
    private final int[] sortTypes;

    /**
     * The order list.
     */
    private final ArrayList<SelectOrderBy> orderList;

    /**
     * Construct a new sort order object.
     *
     * @param database the database
     * @param queryColumnIndexes the column index list
     * @param sortType the sort order bit masks
     * @param orderList the original query order list (if this is a query)
     */
    public SortOrder(Database database, int[] queryColumnIndexes,
            int[] sortType, ArrayList<SelectOrderBy> orderList) {
        this.database = database;
        this.queryColumnIndexes = queryColumnIndexes;
        this.sortTypes = sortType;
        this.orderList = orderList;
    }

    /**
     * Create the SQL snippet that describes this sort order.
     * This is the SQL snippet that usually appears after the ORDER BY clause.
     *
     * @param list the expression list
     * @param visible the number of columns in the select list
     * @return the SQL snippet
     */
    public String getSQL(Expression[] list, int visible) {
        StatementBuilder buff = new StatementBuilder();
        int i = 0;
        for (int idx : queryColumnIndexes) {
            buff.appendExceptFirst(", ");
            if (idx < visible) {
                buff.append(idx + 1);
            } else {
                buff.append('=').append(StringUtils.unEnclose(list[idx].getSQL()));
            }
            int type = sortTypes[i++];
            if ((type & DESCENDING) != 0) {
                buff.append(" DESC");
            }
            if ((type & NULLS_FIRST) != 0) {
                buff.append(" NULLS FIRST");
            } else if ((type & NULLS_LAST) != 0) {
                buff.append(" NULLS LAST");
            }
        }
        return buff.toString();
    }

    /**
     * Compare two expressions where one of them is NULL.
     *
     * @param aNull whether the first expression is null
     * @param sortType the sort bit mask to use
     * @return the result of the comparison (-1 meaning the first expression
     *         should appear before the second, 0 if they are equal)
     */
    public static int compareNull(boolean aNull, int sortType) {
        if ((sortType & NULLS_FIRST) != 0) {
            return aNull ? -1 : 1;
        } else if ((sortType & NULLS_LAST) != 0) {
            return aNull ? 1 : -1;
        } else {
            // see also JdbcDatabaseMetaData.nullsAreSorted*
            int comp = aNull ? DEFAULT_NULL_SORT : -DEFAULT_NULL_SORT;
            return (sortType & DESCENDING) == 0 ? comp : -comp;
        }
    }

    /**
     * Compare two expression lists.
     *
     * @param a the first expression list
     * @param b the second expression list
     * @return the result of the comparison
     */
    @Override
    public int compare(Value[] a, Value[] b) {
        for (int i = 0, len = queryColumnIndexes.length; i < len; i++) {
            int idx = queryColumnIndexes[i];
            int type = sortTypes[i];
            Value ao = a[idx];
            Value bo = b[idx];
            boolean aNull = ao == ValueNull.INSTANCE, bNull = bo == ValueNull.INSTANCE;
            if (aNull || bNull) {
                if (aNull == bNull) {
                    continue;
                }
                return compareNull(aNull, type);
            }
            int comp = database.compare(ao, bo);
            if (comp != 0) {
                return (type & DESCENDING) == 0 ? comp : -comp;
            }
        }
        return 0;
    }

    /**
     * Sort a list of rows.
     *
     * @param rows the list of rows
     */
    public void sort(ArrayList<Value[]> rows) {
        Collections.sort(rows, this);
    }

    /**
     * Sort a list of rows using offset and limit.
     *
     * @param rows the list of rows
     * @param offset the offset
     * @param limit the limit
     */
    public void sort(ArrayList<Value[]> rows, int offset, int limit) {
        int rowsSize = rows.size();
        if (rows.isEmpty() || offset >= rowsSize || limit == 0) {
            return;
        }
        if (offset < 0) {
            offset = 0;
        }
        if (offset + limit > rowsSize) {
            limit = rowsSize - offset;
        }
        if (limit == 1 && offset == 0) {
            rows.set(0, Collections.min(rows, this));
            return;
        }
        Value[][] arr = rows.toArray(new Value[0][]);
        Utils.sortTopN(arr, offset, limit, this);
        for (int i = 0, end = Math.min(offset + limit, rowsSize); i < end; i++) {
            rows.set(i, arr[i]);
        }
    }

    /**
     * Get the column index list. This is the column indexes of the order by
     * expressions within the query.
     * <p>
     * For the query "select name, id from test order by id, name" this is {1,
     * 0} as the first order by expression (the column "id") is the second
     * column of the query, and the second order by expression ("name") is the
     * first column of the query.
     *
     * @return the list
     */
    public int[] getQueryColumnIndexes() {
        return queryColumnIndexes;
    }

    /**
     * Get the column for the given table filter, if the sort column is for this
     * filter.
     *
     * @param index the column index (0, 1,..)
     * @param filter the table filter
     * @return the column, or null
     */
    public Column getColumn(int index, TableFilter filter) {
        if (orderList == null) {
            return null;
        }
        SelectOrderBy order = orderList.get(index);
        Expression expr = order.expression;
        if (expr == null) {
            return null;
        }
        expr = expr.getNonAliasExpression();
        if (expr.isConstant()) {
            return null;
        }
        if (!(expr instanceof ExpressionColumn)) {
            return null;
        }
        ExpressionColumn exprCol = (ExpressionColumn) expr;
        if (exprCol.getTableFilter() != filter) {
            return null;
        }
        return exprCol.getColumn();
    }

    /**
     * Get the sort order bit masks.
     *
     * @return the list
     */
    public int[] getSortTypes() {
        return sortTypes;
    }

    /**
     * Returns sort order bit masks with {@link #NULLS_FIRST} or {@link #NULLS_LAST}
     * explicitly set, depending on {@link SysProperties#SORT_NULLS_HIGH}.
     *
     * @return bit masks with either {@link #NULLS_FIRST} or {@link #NULLS_LAST} explicitly set.
     */
    public int[] getSortTypesWithNullPosition() {
        final int[] sortTypes = this.sortTypes.clone();
        for (int i=0, length = sortTypes.length; i<length; i++) {
            sortTypes[i] = addExplicitNullPosition(sortTypes[i]);
        }
        return sortTypes;
    }

    /**
     * Returns a sort type bit mask with {@link #NULLS_FIRST} or {@link #NULLS_LAST}
     * explicitly set, depending on {@link SysProperties#SORT_NULLS_HIGH}.
     *
     * @param sortType sort type bit mask
     * @return bit mask with either {@link #NULLS_FIRST} or {@link #NULLS_LAST} explicitly set.
     */
    public static int addExplicitNullPosition(final int sortType) {
        if ((sortType & NULLS_FIRST) != NULLS_FIRST && (sortType & NULLS_LAST) != NULLS_LAST) {
            return sortType | ((sortType & DESCENDING) == ASCENDING ? DEFAULT_NULLS_LAST : DEFAULT_NULLS_FIRST);
        } else {
            return sortType;
        }
    }
}
