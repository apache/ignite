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

import java.util.ArrayList;
import java.util.List;
import org.h2.util.StatementBuilder;
import org.h2.util.StringUtils;

/**
 * Plain SELECT query.
 */
public class GridSqlSelect extends GridSqlQuery {
    /** */
    private List<GridSqlElement> cols = new ArrayList<>();

    /** */
    private int visibleCols;

    /** */
    private int[] grpCols;

    /** */
    private GridSqlElement from;

    /** */
    private GridSqlElement where;

    /** */
    private int havingCol = -1;

    /** {@inheritDoc} */
    @Override public int visibleColumns() {
        return visibleCols;
    }

    /**
     * @return Number of columns is select including invisible ones.
     */
    public int allColumns() {
        return cols.size();
    }

    /** {@inheritDoc} */
    @Override protected GridSqlElement column(int col) {
        return cols.get(col);
    }

    /** {@inheritDoc} */
    @Override public String getSQL() {
        StatementBuilder buff = new StatementBuilder(explain() ? "EXPLAIN SELECT" : "SELECT");

        if (distinct)
            buff.append(" DISTINCT");

        for (GridSqlElement expression : columns(true)) {
            buff.appendExceptFirst(",");
            buff.append('\n');
            buff.append(expression.getSQL());
        }

        buff.append("\nFROM ").append(from.getSQL());

        if (where != null)
            buff.append("\nWHERE ").append(StringUtils.unEnclose(where.getSQL()));

        if (grpCols != null) {
            buff.append("\nGROUP BY ");

            buff.resetCount();

            for (int grpCol : grpCols) {
                buff.appendExceptFirst(", ");

                addAlias(buff, cols.get(grpCol));
            }
        }

        if (havingCol >= 0) {
            buff.append("\nHAVING ");

            addAlias(buff, cols.get(havingCol));
        }

        getSortLimitSQL(buff);

        return buff.toString();
    }

    /**
     * @param buff Statement builder.
     * @param expression Alias expression.
     */
    private static void addAlias(StatementBuilder buff, GridSqlElement expression) {
        if (expression instanceof GridSqlAlias)
            expression = expression.child();

        buff.append(StringUtils.unEnclose(expression.getSQL()));
    }

    /**
     * @param visibleOnly If only visible expressions needed.
     * @return Select clause expressions.
     */
    public Iterable<GridSqlElement> columns(boolean visibleOnly) {
        assert visibleCols <= cols.size();

        return visibleOnly && visibleCols != cols.size() ?
            cols.subList(0, visibleCols) : cols;
    }

    /**
     * Clears select expressions list.
     * @return {@code this}.
     */
    public GridSqlSelect clearColumns() {
        visibleCols = 0;
        cols = new ArrayList<>();

        return this;
    }

    /**
     * @param expression Expression.
     * @param visible Expression is visible in select phrase.
     * @return {@code this}.
     */
    public GridSqlSelect addColumn(GridSqlElement expression, boolean visible) {
        if (expression == null)
            throw new NullPointerException();

        if (visible) {
            if (visibleCols != cols.size())
                throw new IllegalStateException("Already started adding invisible columns.");

            visibleCols++;
        }

        cols.add(expression);

        return this;
    }

    /**
     * @param colIdx Column index.
     * @param expression Expression.
     * @return {@code this}.
     */
    public GridSqlSelect setColumn(int colIdx, GridSqlElement expression) {
        if (expression == null)
            throw new NullPointerException();

        cols.set(colIdx, expression);

        return this;
    }

    /**
     * @return Group columns.
     */
    public int[] groupColumns() {
        return grpCols;
    }

    /**
     * @param grpCols Group columns.
     * @return {@code this}.
     */
    public GridSqlSelect groupColumns(int[] grpCols) {
        this.grpCols = grpCols;

        return this;
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
     * @return {@code this}.
     */
    public GridSqlSelect where(GridSqlElement where) {
        this.where = where;

        return this;
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
        return havingCol >= 0 ? column(havingCol) : null;
    }

    /**
     * @param col Index of HAVING column.
     * @return {@code this}.
     */
    public GridSqlSelect havingColumn(int col) {
        assert col >= -1 : col;

        havingCol = col;

        return this;
    }

    /**
     * @return Index of HAVING column.
     */
    public int havingColumn() {
        return havingCol;
    }
}