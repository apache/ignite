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
 * Plain SELECT query.
 */
public class GridSqlSelect extends GridSqlQuery {
    /** */
    private List<GridSqlElement> allExprs = new ArrayList<>();

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

    /** {@inheritDoc} */
    @Override public int visibleColumns() {
        return select.size();
    }

    /**
     * @return Number of columns is select including invisible ones.
     */
    public int allColumns() {
        return allExprs.size();
    }

    /** {@inheritDoc} */
    @Override protected GridSqlElement expression(int col) {
        return allExprs.get(col);
    }

    /** {@inheritDoc} */
    @Override public String getSQL() {
        StatementBuilder buff = new StatementBuilder(explain() ? "EXPLAIN SELECT" : "SELECT");

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

        getSortLimitSQL(buff);

        return buff.toString();
    }

    /**
     * @param visibleOnly If only visible expressions needed.
     * @return Select phrase expressions.
     */
    public Iterable<GridSqlElement> select(boolean visibleOnly) {
        return visibleOnly ? select : allExprs;
    }

    /**
     * Clears select list.
     */
    public void clearSelect() {
        select = new ArrayList<>();
        allExprs = new ArrayList<>();
    }

    /**
     * @param expression Expression.
     * @param visible Expression is visible in select phrase.
     */
    public void addSelectExpression(GridSqlElement expression, boolean visible) {
        if (expression == null)
            throw new NullPointerException();

        if (visible) {
            if (select.size() != allExprs.size())
                throw new IllegalStateException("Already started adding invisible columns.");

            select.add(expression);
        }
        else if (select.isEmpty())
            throw new IllegalStateException("No visible columns.");

        allExprs.add(expression);
    }

    /**
     * @param colIdx Column index.
     * @param expression Expression.
     */
    public void setSelectExpression(int colIdx, GridSqlElement expression) {
        if (expression == null)
            throw new NullPointerException();

        if (colIdx < select.size()) // Assuming that all the needed expressions were already added.
            select.set(colIdx, expression);

        allExprs.set(colIdx, expression);
    }

    /**
     * @return Expressions.
     */
    public Iterable<GridSqlElement> groups() {
        return groups;
    }

    /**
     * @return {@code true} If the select has group by expression.
     */
    public boolean hasGroupBy() {
        return !groups.isEmpty();
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
     * @param cond Adds new WHERE condition using AND operator.
     * @return {@code this}.
     */
    public GridSqlSelect whereAnd(GridSqlElement cond) {
        if (cond == null)
            throw new NullPointerException();

        GridSqlElement old = where();

        where(old == null ? cond : new GridSqlOperation(GridSqlOperationType.AND, old, cond));

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
        havingCol = col;
    }

    /**
     * @return Index of HAVING column.
     */
    public int havingColumn() {
        return havingCol;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneCallsConstructors", "CloneDoesntDeclareCloneNotSupportedException"})
    @Override public GridSqlSelect clone() {
        GridSqlSelect res = (GridSqlSelect)super.clone();

        res.groups = new ArrayList<>(groups);
        res.grpCols =  grpCols == null ? null : grpCols.clone();
        res.select = new ArrayList<>(select);
        res.allExprs = new ArrayList<>(allExprs);

        return res;
    }
}
