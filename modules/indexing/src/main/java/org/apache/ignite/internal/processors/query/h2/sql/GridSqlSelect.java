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
import java.util.Set;

import org.h2.util.StatementBuilder;
import org.h2.util.StringUtils;

/**
 * Plain SELECT query.
 */
public class GridSqlSelect extends GridSqlQuery {
    /** */
    public static final int FROM_CHILD = 2;

    /** */
    public static final int WHERE_CHILD = 3;

    /** */
    private static final int COLS_CHILD = 4;

    /** */
    private List<GridSqlAst> cols = new ArrayList<>();

    /** */
    private int visibleCols;

    /** */
    private boolean distinct;

    /** */
    private int[] grpCols;

    /** */
    private GridSqlAst from;

    /** */
    private GridSqlAst where;

    /** */
    private int havingCol = -1;

    /** */
    private boolean isForUpdate;

    /**
     * @param colIdx Column index as for {@link #column(int)}.
     * @return Child index for {@link #child(int)}.
     */
    public static int childIndexForColumn(int colIdx) {
        return colIdx + COLS_CHILD;
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return 4 + cols.size(); // + FROM + WHERE + OFFSET + LIMIT
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <E extends GridSqlAst> E child(int childIdx) {
        if (childIdx < FROM_CHILD)
            return super.child(childIdx);

        switch (childIdx) {
            case FROM_CHILD:
                return maskNull(from, GridSqlPlaceholder.EMPTY);

            case WHERE_CHILD:
                return maskNull(where, GridSqlConst.TRUE);

            default:
                return (E)cols.get(childIdx - COLS_CHILD);
        }
    }

    /** {@inheritDoc} */
    @Override public <E extends GridSqlAst> void child(int childIdx, E child) {
        if (childIdx < FROM_CHILD) {
            super.child(childIdx, child);

            return;
        }

        switch (childIdx) {
            case FROM_CHILD:
                from = child;

                break;

            case WHERE_CHILD:
                where = child;

                break;

            default:
                cols.set(childIdx - COLS_CHILD, child);
        }
    }

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
    @Override protected GridSqlAst column(int col) {
        return cols.get(col);
    }

    /** {@inheritDoc} */
    @Override public String getSQL() {
        StatementBuilder buff = new StatementBuilder(explain() ? "EXPLAIN SELECT" : "SELECT");

        if (distinct)
            buff.append(" DISTINCT");

        for (GridSqlAst expression : columns(true)) {
            buff.appendExceptFirst(",");
            buff.append('\n');
            buff.append(expression.getSQL());
        }

        if (from != null)
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

        if (isForUpdate)
            buff.append("\nFOR UPDATE");

        return buff.toString();
    }

    /**
     * @return {@code True} if this simple SQL query like 'SELECT A, B, C from SOME_TABLE' without any conditions
     *      and expressions.
     */
    @Override public boolean skipMergeTable() {
        boolean simple = !distinct &&
            from instanceof GridSqlTable &&
            where == null &&
            grpCols == null &&
            havingCol < 0 &&
            sort.isEmpty() &&
            limit() == null &&
            offset() == null;

        if (simple) {
            for (GridSqlAst expression : columns(true)) {
                if (expression instanceof GridSqlAlias)
                    expression = expression.child();

                if (!(expression instanceof GridSqlColumn))
                    return false;
            }
        }

        return simple;
    }

    /**
     * @param buff Statement builder.
     * @param exp Alias expression.
     */
    private static void addAlias(StatementBuilder buff, GridSqlAst exp) {
        exp = GridSqlAlias.unwrap(exp);

        buff.append(StringUtils.unEnclose(exp.getSQL()));
    }

    /**
     * @param visibleOnly If only visible expressions needed.
     * @return Select clause expressions.
     */
    public List<GridSqlAst> columns(boolean visibleOnly) {
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
    public GridSqlSelect addColumn(GridSqlAst expression, boolean visible) {
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
    public GridSqlSelect setColumn(int colIdx, GridSqlAst expression) {
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
    public GridSqlAst from() {
        return from;
    }

    /**
     * @param from From element.
     * @return {@code this}.
     */
    public GridSqlSelect from(GridSqlAst from) {
        this.from = from;

        return this;
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
     * @return Where.
     */
    public GridSqlAst where() {
        return where;
    }

    /**
     * @param where New where.
     * @return {@code this}.
     */
    public GridSqlSelect where(GridSqlAst where) {
        this.where = where;

        return this;
    }

    /**
     * @param cond Adds new WHERE condition using AND operator.
     * @return {@code this}.
     */
    public GridSqlSelect whereAnd(GridSqlAst cond) {
        if (cond == null)
            throw new NullPointerException();

        GridSqlAst old = where();

        where(old == null ? cond : new GridSqlOperation(GridSqlOperationType.AND, old, cond));

        return this;
    }

    /**
     * @return Having.
     */
    public GridSqlAst having() {
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
     * @return Whether this statement is {@code FOR UPDATE}.
     */
    public boolean isForUpdate() {
        return isForUpdate;
    }

    /**
     * @param forUpdate Whether this statement is {@code FOR UPDATE}.
     */
    public void forUpdate(boolean forUpdate) {
        isForUpdate = forUpdate;
    }

    /**
     * @return Index of HAVING column.
     */
    public int havingColumn() {
        return havingCol;
    }

    /**
     * Collect aliases from FROM part.
     *
     * @param aliases Table aliases in FROM.
     */
    public void collectFromAliases(Set<GridSqlAlias> aliases) {
        GridSqlAst from = from();

        if (from == null)
            return;

        while (from instanceof GridSqlJoin) {
            GridSqlElement right = ((GridSqlJoin)from).rightTable();

            aliases.add((GridSqlAlias)right);

            from = ((GridSqlJoin)from).leftTable();
        }

        aliases.add((GridSqlAlias)from);
    }

    /**
     * @return Copy of this select for SELECT FOR UPDATE specific tasks.
     */
    public GridSqlSelect copySelectForUpdate() {
        assert isForUpdate && !distinct && havingCol < 0 && grpCols == null; // Not supported by SFU.

        GridSqlSelect copy = new GridSqlSelect();

        copy.from(from())
            .where(where());

        int vis = visibleColumns();

        for (int i = 0; i < columns(false).size(); i++)
            copy.addColumn(column(i), i < vis);

        if (!sort().isEmpty()) {
            for (GridSqlSortColumn sortCol : sort())
                copy.addSort(sortCol);
        }

        return copy;
    }
}
