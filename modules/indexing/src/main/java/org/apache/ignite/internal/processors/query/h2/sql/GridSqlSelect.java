/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.h2.sql;

import org.h2.util.*;

import java.util.*;

/**
 * Select query.
 */
public class GridSqlSelect implements Cloneable {
    /** */
    private boolean distinct;

    /** */
    private List<GridSqlElement> allExprs;

    /** */
    private List<GridSqlElement> select = new ArrayList<>();

    /** */
    private List<GridSqlElement> groups = new ArrayList<>();

    /** */
    private int[] grpCols;

    /** */
    private GridSqlElement from;

    /** */
    private GridSqlElement where;

    /** */
    private GridSqlElement having;

    /** */
    private int havingCol = -1;

    /** */
    private Map<GridSqlElement,GridSqlSortColumn> sort = new LinkedHashMap<>();

    /** */
    private GridSqlElement offset;

    /** */
    private GridSqlElement limit;

    /**
     * @return Offset.
     */
    public GridSqlElement offset() {
        return offset;
    }

    /**
     * @param offset Offset.
     */
    public void offset(GridSqlElement offset) {
        this.offset = offset;
    }

    /**
     * @param limit Limit.
     */
    public void limit(GridSqlElement limit) {
        this.limit = limit;
    }

    /**
     * @return Limit.
     */
    public GridSqlElement limit() {
        return limit;
    }

    /**
     * @return Distinct.
     */
    public boolean distinct() {
        return distinct;
    }

    /**
     * @param distinct New distinct.
     */
    public void distinct(boolean distinct) {
        this.distinct = distinct;
    }

    /**
     * @return Generate sql.
     */
    public String getSQL() {
        StatementBuilder buff = new StatementBuilder("SELECT");

        if (distinct)
            buff.append(" DISTINCT");

        for (GridSqlElement expression : select) {
            buff.appendExceptFirst(",");
            buff.append('\n');
            buff.append(StringUtils.indent(expression.getSQL(), 4, false));
        }

        buff.append("\nFROM ").append(from.getSQL());

        if (where != null)
            buff.append("\nWHERE ").append(StringUtils.unEnclose(where.getSQL()));

        if (!groups.isEmpty()) {
            buff.append("\nGROUP BY ");

            buff.resetCount();

            for (GridSqlElement expression : groups) {
                buff.appendExceptFirst(", ");

                if (expression instanceof GridSqlAlias)
                    buff.append(StringUtils.unEnclose((expression.child().getSQL())));
                else
                    buff.append(StringUtils.unEnclose(expression.getSQL()));
            }
        }

        if (having != null)
            buff.append("\nHAVING ").append(StringUtils.unEnclose(having.getSQL()));

        if (!sort.isEmpty()) {
            buff.append("\nORDER BY ");

            buff.resetCount();

            for (Map.Entry<GridSqlElement,GridSqlSortColumn> entry : sort.entrySet()) {
                buff.appendExceptFirst(", ");

                GridSqlElement expression = entry.getKey();

                int idx = select.indexOf(expression);

                if (idx >= 0)
                    buff.append(idx + 1);
                else
                    buff.append('=').append(StringUtils.unEnclose(expression.getSQL()));

                GridSqlSortColumn type = entry.getValue();

                if (!type.asc())
                    buff.append(" DESC");

                if (type.nullsFirst())
                    buff.append(" NULLS FIRST");
                else if (type.nullsLast())
                    buff.append(" NULLS LAST");
            }
        }

        if (limit != null)
            buff.append(" LIMIT ").append(StringUtils.unEnclose(limit.getSQL()));

        if (offset != null)
            buff.append(" OFFSET ").append(StringUtils.unEnclose(offset.getSQL()));

        return buff.toString();
    }

    /**
     * @param expression Expression.
     */
    public void addExpression(GridSqlElement expression) {
        if (allExprs == null)
            allExprs = new ArrayList<>();

        allExprs.add(expression);
    }

    /**
     * @return All expressions in select, group by, order by.
     */
    public List<GridSqlElement> allExpressions() {
        return allExprs;
    }

    /**
     * @return Expressions.
     */
    public List<GridSqlElement> select() {
        return select;
    }

    /**
     * Clears select list.
     */
    public void clearSelect() {
        select = new ArrayList<>();
    }

    /**
     * @param expression Expression.
     */
    public void addSelectExpression(GridSqlElement expression) {
        if (expression == null)
            throw new NullPointerException();

        select.add(expression);
    }

    /**
     * @return Expressions.
     */
    public List<GridSqlElement> groups() {
        return groups;
    }

    /**
     *
     */
    public void clearGroups() {
        groups = new ArrayList<>();
        grpCols = null;
    }

    /**
     * @param expression Expression.
     */
    public void addGroupExpression(GridSqlElement expression) {
        if (expression == null)
            throw new NullPointerException();

        groups.add(expression);
    }

    /**
     * @return Group columns.
     */
    public int[] groupColumns() {
        return grpCols;
    }

    /**
     * @param grpCols Group columns.
     */
    public void groupColumns(int[] grpCols) {
        this.grpCols = grpCols;
    }

    /**
     * @return Tables.
     */
    public GridSqlElement from() {
        return from;
    }

    /**
     * @param from From element.
     * @return {@code this}.
     */
    public GridSqlSelect from(GridSqlElement from) {
        this.from = from;

        return this;
    }

    /**
     * @return Where.
     */
    public GridSqlElement where() {
        return where;
    }

    /**
     * @param where New where.
     */
    public void where(GridSqlElement where) {
        this.where = where;
    }

    /**
     * @param condition Adds new WHERE condition using AND operator.
     * @return {@code this}.
     */
    public GridSqlSelect whereAnd(GridSqlElement condition) {
        if (condition == null)
            throw new NullPointerException();

        GridSqlElement old = where();

        where(old == null ? condition : new GridSqlOperation(GridSqlOperationType.AND, old, condition));

        return this;
    }

    /**
     * @return Having.
     */
    public GridSqlElement having() {
        return having;
    }

    /**
     * @param having New having.
     */
    public void having(GridSqlElement having) {
        this.having = having;
    }

    /**
     * @param col Index of HAVING column.
     */
    public void havingColumn(int col) {
        this.havingCol = col;
    }

    /**
     * @return Index of HAVING column.
     */
    public int havingColumn() {
        return havingCol;
    }

    /**
     * @return Sort.
     */
    public Map<GridSqlElement,GridSqlSortColumn> sort() {
        return sort;
    }

    /**
     *
     */
    public void clearSort() {
        sort = new LinkedHashMap<>();
    }

    /**
     * @param expression Expression.
     * @param sortType The sort type bit mask (SortOrder.DESCENDING, SortOrder.NULLS_FIRST, SortOrder.NULLS_LAST).
     */
    public void addSort(GridSqlElement expression, GridSqlSortColumn sortType) {
        sort.put(expression, sortType);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneCallsConstructors", "CloneDoesntDeclareCloneNotSupportedException"})
    @Override public GridSqlSelect clone() {
        try {
            GridSqlSelect res = (GridSqlSelect)super.clone();

            res.select = new ArrayList<>(select);
            res.groups = new ArrayList<>(groups);
            res.sort = new LinkedHashMap<>(sort);
            res.allExprs = null;

            return res;
        }
        catch (CloneNotSupportedException e) {
            throw new RuntimeException(e); // Never thrown.
        }
    }
}
