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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.query.GridCacheSqlQuery;
import org.apache.ignite.internal.processors.cache.query.GridCacheTwoStepQuery;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.h2.command.Prepared;
import org.h2.command.dml.Query;
import org.h2.command.dml.SelectUnion;
import org.h2.jdbc.JdbcPreparedStatement;
import org.h2.util.IntArray;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing.setupConnection;
import static org.apache.ignite.internal.processors.query.h2.opt.GridH2CollocationModel.isCollocated;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlConst.TRUE;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlFunctionType.AVG;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlFunctionType.CAST;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlFunctionType.COUNT;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlFunctionType.MAX;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlFunctionType.MIN;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlFunctionType.SUM;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlJoin.LEFT_TABLE_CHILD;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlJoin.ON_CHILD;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlJoin.RIGHT_TABLE_CHILD;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlPlaceholder.EMPTY;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlQuery.LIMIT_CHILD;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlQuery.OFFSET_CHILD;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlQueryParser.prepared;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlSelect.FROM_CHILD;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlSelect.WHERE_CHILD;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlSelect.childIndexForColumn;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlUnion.LEFT_CHILD;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlUnion.RIGHT_CHILD;
import static org.apache.ignite.internal.processors.query.h2.twostep.GridReduceQueryExecutor.toArray;

/**
 * Splits a single SQL query into two step map-reduce query.
 */
public class GridSqlQuerySplitter {
    /** */
    private static final String MERGE_TABLE_SCHEMA = "PUBLIC"; // Schema PUBLIC must always exist.

    /** */
    private static final String MERGE_TABLE_PREFIX = "__T";

    /** */
    private static final String COLUMN_PREFIX = "__C";

    /** */
    private static final String HAVING_COLUMN = "__H";

    /** */
    private static final String UNIQUE_TABLE_ALIAS_SUFFIX = "__Z";

    /** */
    private int nextTblAliasId;

    /** */
    private int splitId = -1; // The first one will be 0.

    /** */
    private Set<String> schemas = new HashSet<>();

    /** */
    private Set<String> tbls = new HashSet<>();

    /** */
    private boolean rdcQrySimple;

    /** */
    private GridCacheSqlQuery rdcSqlQry;

    /** */
    private List<GridCacheSqlQuery> mapSqlQrys = new ArrayList<>();

    /** */
    private Object[] params;

    /** */
    private boolean collocatedGrpBy;

    /** */
    private IdentityHashMap<GridSqlAst, GridSqlAlias> uniqueFromAliases = new IdentityHashMap<>();

    /**
     * @param params Query parameters.
     * @param collocatedGrpBy If it is a collocated GROUP BY query.
     */
    public GridSqlQuerySplitter(Object[] params, boolean collocatedGrpBy) {
        this.params = params;
        this.collocatedGrpBy = collocatedGrpBy;
    }

    /**
     * @param idx Index of table.
     * @return Merge table.
     */
    private static GridSqlTable mergeTable(int idx) {
        return new GridSqlTable(MERGE_TABLE_SCHEMA, MERGE_TABLE_PREFIX + idx);
    }

    /**
     * @param idx Table index.
     * @return Merge table name.
     */
    public static String mergeTableIdentifier(int idx) {
        return mergeTable(idx).getSQL();
    }

    /**
     * @param idx Index of column.
     * @return Generated by index column alias.
     */
    private String columnName(int idx) {
        // We must have unique columns for each split to avoid name clashes.
        return COLUMN_PREFIX + splitId + '_' + idx;
    }

    /**
     * @param stmt Prepared statement.
     * @param params Parameters.
     * @param collocatedGrpBy Whether the query has collocated GROUP BY keys.
     * @param distributedJoins If distributed joins enabled.
     * @param enforceJoinOrder Enforce join order.
     * @param h2 Indexing.
     * @return Two step query.
     * @throws SQLException If failed.
     * @throws IgniteCheckedException If failed.
     */
    public static GridCacheTwoStepQuery split(
        JdbcPreparedStatement stmt,
        Object[] params,
        boolean collocatedGrpBy,
        boolean distributedJoins,
        boolean enforceJoinOrder,
        IgniteH2Indexing h2
    ) throws SQLException, IgniteCheckedException {
        if (params == null)
            params = GridCacheSqlQuery.EMPTY_PARAMS;

        // Here we will just do initial query parsing. Do not use optimized
        // subqueries because we do not have unique FROM aliases yet.
        GridSqlQuery qry = parse(prepared(stmt), false);

        String originalSql = qry.getSQL();

        final boolean explain = qry.explain();

        qry.explain(false);

        GridSqlQuerySplitter splitter = new GridSqlQuerySplitter(params, collocatedGrpBy);

        // Normalization will generate unique aliases for all the table filters in FROM.
        // Also it will collect all tables and schemas from the query.
        splitter.normalizeQuery(qry);

        Connection conn = stmt.getConnection();

        // Here we will have correct normalized AST with optimized join order.
        // The distributedJoins parameter is ignored because it is not relevant for
        // the REDUCE query optimization.
        qry = parse(optimize(h2, conn, qry.getSQL(), params, false, enforceJoinOrder),
            true);

        // Do the actual query split. We will update the original query AST, need to be careful.
        splitter.splitQuery(qry);

        assert !F.isEmpty(splitter.mapSqlQrys): "map"; // We must have at least one map query.
        assert splitter.rdcSqlQry != null: "rdc"; // We must have a reduce query.

        // If we have distributed joins, then we have to optimize all MAP side queries
        // to have a correct join order with respect to batched joins and check if we need
        // distributed joins at all.
        if (distributedJoins) {
            boolean allCollocated = true;

            for (GridCacheSqlQuery mapSqlQry : splitter.mapSqlQrys) {
                Prepared prepared = optimize(h2, conn, mapSqlQry.query(), mapSqlQry.parameters(),
                    true, enforceJoinOrder);

                allCollocated &= isCollocated((Query)prepared);

                mapSqlQry.query(parse(prepared, true).getSQL());
            }

            // We do not need distributed joins if all MAP queries are collocated.
            if (allCollocated)
                distributedJoins = false;
        }

        // Setup resulting two step query and return it.
        GridCacheTwoStepQuery twoStepQry = new GridCacheTwoStepQuery(originalSql, splitter.schemas, splitter.tbls);

        twoStepQry.reduceQuery(splitter.rdcSqlQry);

        for (GridCacheSqlQuery mapSqlQry : splitter.mapSqlQrys)
            twoStepQry.addMapQuery(mapSqlQry);

        twoStepQry.skipMergeTable(splitter.rdcQrySimple);
        twoStepQry.explain(explain);
        twoStepQry.distributedJoins(distributedJoins);

        return twoStepQry;
    }

    /**
     * @param qry Optimized and normalized query to split.
     */
    private void splitQuery(GridSqlQuery qry) {
        // Create a fake parent AST element for the query to allow replacing the query in the parent by split.
        GridSqlSubquery fakeQryPrnt = new GridSqlSubquery(qry);

        // Fake parent query model. We need it just for convenience because buildQueryModel needs parent model.
        QueryModel fakeQrymPrnt = new QueryModel(null, null, -1, null);

        // Build a simplified query model. We need it because navigation over the original AST is too complex.
        buildQueryModel(fakeQrymPrnt, fakeQryPrnt, 0, null);

        assert fakeQrymPrnt.size() == 1;

        // Get the built query model from the fake parent.
        QueryModel qrym = fakeQrymPrnt.get(0);

        // Setup the needed information for split.
        analyzeQueryModel(qrym);

        // If we have child queries to split, then go hard way.
        if (qrym.needSplitChild) {
            // All the siblings to selects we are going to split must be also wrapped into subqueries.
            pushDownQueryModel(qrym);

            // Need to make all the joined subqueries to be ordered by join conditions.
            setupMergeJoinSorting(qrym);
        }
        else if (!qrym.needSplit)  // Just split the top level query.
            setNeedSplit(qrym);

        // Split the query model into multiple map queries and a single reduce query.
        splitQueryModel(qrym);

        // Get back the updated query from the fake parent. It will be our reduce query.
        qry = fakeQryPrnt.subquery();

        // Setup a resulting reduce query.
        rdcSqlQry = new GridCacheSqlQuery(qry.getSQL());
        rdcQrySimple = qry.simpleQuery();

        setupParameters(rdcSqlQry, qry, params);
    }

    /**
     * @param qrym Query model.
     */
    private void pushDownQueryModel(QueryModel qrym) {
        if (qrym.type == Type.UNION) {
            assert qrym.needSplitChild; // Otherwise we should not get here.

            for (int i = 0; i < qrym.size(); i++)
                pushDownQueryModel(qrym.get(i));
        }
        else if (qrym.type == Type.SELECT) {
            // If we already need to split, then no need to push down here.
            if (!qrym.needSplit) {
                assert qrym.needSplitChild; // Otherwise we should not get here.

                pushDownQueryModelSelect(qrym);
            }
        }
        else
            throw new IllegalStateException("Type: " + qrym.type);
    }

    /**
     * @param qrym Query model for the SELECT.
     */
    private void pushDownQueryModelSelect(QueryModel qrym) {
        int begin = -1;

        // Here we iterate over joined FROM table filters.
        for (int i = 0; i < qrym.size(); i++) {
            QueryModel child = qrym.get(i);

            if (child.isQuery() && (child.needSplitChild || child.needSplit)) {
                // Push down the currently collected range.
                if (begin != -1) {
                    pushDownQueryModelRange(qrym, begin, i - 1);

                    i = begin + 1; // We've modified qrym by this range push down, need to adjust counter.

                    assert qrym.get(i) == child; // Adjustment check: we have to return to the same point.

                    // Reset range begin.
                    begin = -1;
                }

                if (child.needSplitChild)
                    pushDownQueryModel(child);
            }
            // It is a table or a function or a subquery that we do not need to split or split child.
            // We need to find all the joined elements like this in chain to push them down.
            else if (begin == -1)
                begin = i;
        }

        // Push down the remaining range.
        if (begin != -1)
            pushDownQueryModelRange(qrym, begin, qrym.size() - 1);
    }

    /**
     * @param qrym Query model.
     */
    private void setupMergeJoinSorting(QueryModel qrym) {
        if (qrym.type == Type.UNION) {
            for (int i = 0; i < qrym.size(); i++)
                setupMergeJoinSorting(qrym.get(i));
        }
        else if (qrym.type == Type.SELECT) {
            if (!qrym.needSplit) {
                boolean needSplitChild = false;

                for (int i = 0; i < qrym.size(); i++) {
                    QueryModel child = qrym.get(i);

                    assert child.isQuery() : qrym.type;

                    if (child.needSplit)
                        needSplitChild = true;
                    else
                        setupMergeJoinSorting(child); // Go deeper.
                }

                if (needSplitChild && qrym.size() > 1)
                    setupMergeJoinSortingSelect(qrym); // Setup merge join hierarchy for the SELECT.
            }
        }
        else
            throw new IllegalStateException("Type: " + qrym.type);
    }

    /**
     * @param qrym Query model.
     */
    private void setupMergeJoinSortingSelect(QueryModel qrym) {
        // After pushing down we can have the following variants:
        //  - splittable SELECT
        //  - subquery with splittable child (including UNION)

        // We will push down chains of sequential subqueries between
        // the splittable SELECT like in "push down" and
        // setup sorting for all of them by joined fields.

        for (int i = 1; i < qrym.size(); i++) {
            QueryModel child = qrym.get(i);

            if (child.needSplit) {
                if (i > 1) {
                    // If we have multiple joined non-splittable subqueries before, then
                    // push down them into a single subquery.
                    doPushDownQueryModelRange(qrym, 0, i - 1, false);

                    i = 1;

                    assert qrym.get(i) == child; // We must remain at the same position.
                }

                injectSortingFirstJoin(qrym);
            }
        }
    }

    /**
     * @param qrym Query model.
     */
    private void injectSortingFirstJoin(QueryModel qrym) {
        GridSqlJoin join = findJoin(qrym, 0);

        // We are at the beginning, thus left and right AST must be children of the same join AST.
        //     join2
        //      / \
        //   join1 \
        //    / \   \
        //  T0   T1  T2
        GridSqlAlias leftTbl = (GridSqlAlias)join.leftTable();
        GridSqlAlias rightTbl = (GridSqlAlias)join.rightTable();

        // Collect all AND conditions.
        List<AndCondition> andConditions = new ArrayList<>();

        collectAndConditions(andConditions, join, ON_CHILD);
        collectAndConditions(andConditions, qrym.ast(), WHERE_CHILD);

        // Collect all the JOIN condition columns for sorting.
        List<GridSqlColumn> leftOrder = new ArrayList<>();
        List<GridSqlColumn> rightOrder = new ArrayList<>();

        for (int i = 0; i < andConditions.size(); i++) {
            AndCondition and = andConditions.get(i);
            GridSqlOperation op = and.ast();

            if (op.operationType() == GridSqlOperationType.EQUAL) {
                GridSqlAst leftExpr = op.child(0);
                GridSqlAst rightExpr = op.child(1);

                if (leftExpr instanceof GridSqlColumn && rightExpr instanceof GridSqlColumn) {
                    GridSqlAst leftFrom = ((GridSqlColumn)leftExpr).expressionInFrom();
                    GridSqlAst rightFrom = ((GridSqlColumn)rightExpr).expressionInFrom();

                    if (leftFrom == leftTbl && rightFrom == rightTbl) {
                        leftOrder.add((GridSqlColumn)leftExpr);
                        rightOrder.add((GridSqlColumn)rightExpr);
                    }
                    else if (leftFrom == rightTbl && rightFrom == leftTbl) {
                        leftOrder.add((GridSqlColumn)rightExpr);
                        rightOrder.add((GridSqlColumn)leftExpr);
                    }
                }
            }
        }

        // Do inject ORDER BY to the both sides.
        injectOrderBy(leftTbl, leftOrder);
        injectOrderBy(rightTbl, rightOrder);
    }

    /**
     * @param subQryAlias Subquery alias.
     * @param orderByCols Columns for ORDER BY.
     */
    private void injectOrderBy(GridSqlAlias subQryAlias, List<GridSqlColumn> orderByCols) {
        if (orderByCols.isEmpty())
            return;

        // Structure: alias -> subquery -> query
        GridSqlQuery qry = GridSqlAlias.<GridSqlSubquery>unwrap(subQryAlias).subquery();
        GridSqlSelect select = leftmostSelect(qry); // The leftmost SELECT in UNION defines column names.

        BitSet set = new BitSet();

        for (int i = 0; i < orderByCols.size(); i++) {
            GridSqlColumn col = orderByCols.get(i);

            int colIdx = 0;

            for (;;) {
                GridSqlAst expr = select.column(colIdx);

                String colName;

                if (expr instanceof GridSqlAlias)
                    colName = ((GridSqlAlias)expr).alias();
                else if (expr instanceof GridSqlColumn)
                    colName = ((GridSqlColumn)expr).columnName();
                else
                    // It must be impossible to join by this column then, because the expression has no name.
                    throw new IllegalStateException();

                if (colName.equals(col.columnName()))
                    break; // Found the needed column index.

                colIdx++;
            }

            // Avoid duplicates.
            if (set.get(colIdx))
                continue;

            set.set(colIdx, true);

            // Add sort column to the query.
            qry.addSort(new GridSqlSortColumn(colIdx, true, false, false));
        }
    }

    /**
     * @param qry Query.
     * @return Leftmost select if it is a UNION query.
     */
    private GridSqlSelect leftmostSelect(GridSqlQuery qry) {
        while (qry instanceof GridSqlUnion)
            qry = ((GridSqlUnion)qry).left();

        return (GridSqlSelect)qry;
    }

    /**
     * @param qrym Query model.
     * @param begin The first child model in range to push down.
     * @param end The last child model in range to push down.
     */
    private void pushDownQueryModelRange(QueryModel qrym, int begin, int end) {
        assert end >= begin;

        if (begin == end && qrym.get(end).isQuery()) {
            // Simple case when we have a single subquery to push down, just mark it to be splittable.
            setNeedSplit(qrym.get(end));
        }
        else {
            // Here we have to generate a subquery for all the joined elements and
            // and mark that subquery as splittable.
            doPushDownQueryModelRange(qrym, begin, end, true);
        }
    }

    /**
     * @param qrym Query model.
     */
    private void setNeedSplit(QueryModel qrym) {
        if (qrym.type == Type.SELECT)
            qrym.needSplit = true;
        else if (qrym.type == Type.UNION) {
            qrym.needSplitChild = true;

            // Mark all the selects in the UNION to be splittable.
            for (QueryModel s : qrym) {
                assert s.type == Type.SELECT: s.type;

                s.needSplit = true;
            }
        }
        else
            throw new IllegalStateException("Type: " + qrym.type);
    }

    /**
     * @param qrym Query model.
     * @param begin The first child model in range to push down.
     * @param end The last child model in range to push down.
     * @param needSplit If we will need to split the created subquery model.
     */
    private void doPushDownQueryModelRange(QueryModel qrym, int begin, int end, boolean needSplit) {
        // Create wrap query where we will push all the needed joined elements, columns and conditions.
        GridSqlSelect wrapSelect = new GridSqlSelect();
        GridSqlSubquery wrapSubqry = new GridSqlSubquery(wrapSelect);
        GridSqlAlias wrapAlias = alias(nextUniqueTableAlias(null), wrapSubqry);

        QueryModel wrapQrym = new QueryModel(Type.SELECT, wrapSubqry, 0, wrapAlias);

        wrapQrym.needSplit = needSplit;

        // Prepare all the prerequisites.
        GridSqlSelect select = qrym.ast();

        Set<GridSqlAlias> tblAliases = newIdentityHashSet();
        Map<String,GridSqlAlias> cols = new HashMap<>();

        // Collect all the tables for push down.
        for (int i = begin; i <= end; i++) {
            GridSqlAlias uniqueTblAlias = qrym.get(i).uniqueAlias;

            assert uniqueTblAlias != null: select.getSQL();

            tblAliases.add(uniqueTblAlias);
        }

        // Push down columns in SELECT clause.
        pushDownSelectColumns(tblAliases, cols, wrapAlias, select);

        // Move all the related WHERE conditions to wrap query.
        pushDownWhereConditions(tblAliases, cols, wrapAlias, select);

        // Push down to a subquery all the JOIN elements and process ON conditions.
        pushDownJoins(tblAliases, cols, qrym, begin, end, wrapAlias);

        // Add all the collected columns to the wrap query.
        for (GridSqlAlias col : cols.values())
            wrapSelect.addColumn(col, true);

        // Adjust query models to a new AST structure.

        // Move pushed down child models to the newly created model.
        for (int i = begin; i <= end; i++) {
            QueryModel child = qrym.get(i);

            wrapQrym.add(child);
        }

        // Replace the first child model with the created one.
        qrym.set(begin, wrapQrym);

        // Drop others.
        for (int x = begin + 1, i = x; i <= end; i++)
            qrym.remove(x);
    }

    /**
     * @param tblAliases Table aliases for push down.
     * @param cols Columns with generated aliases.
     * @param qrym Query model.
     * @param begin The first child model in range to push down.
     * @param end The last child model in range to push down.
     * @param wrapAlias Alias of the wrap query.
     */
    private void pushDownJoins(
        Set<GridSqlAlias> tblAliases,
        Map<String,GridSqlAlias> cols,
        QueryModel qrym,
        int begin,
        int end,
        GridSqlAlias wrapAlias
    ) {
        GridSqlSelect wrapSelect = GridSqlAlias.<GridSqlSubquery>unwrap(wrapAlias).subquery();

        final int last = qrym.size() - 1;

        if (begin == end) {
            //  Simple case when we have to push down only a single table and no joins.

            //       join3
            //        / \
            //     join2 \
            //      / \   \
            //   join1 \   \
            //    / \   \   \
            //  T0   T1  T2  T3
            //           ^^

            // - Push down T2 to the wrap query W and replace T2 with W:

            //       join3
            //        / \
            //     join2 \
            //      / \   \
            //   join1 \   \
            //    / \   \   \
            //  T0   T1  W   T3

            //  W: T2

            GridSqlJoin endJoin = findJoin(qrym, end);

            wrapSelect.from(qrym.get(end).uniqueAlias);
            endJoin.child(end == 0 ? LEFT_TABLE_CHILD : RIGHT_TABLE_CHILD, wrapAlias);
        }
        else if (end == last) {
            // Here we need to push down chain from `begin` to the last child in the model (T3).
            // Thus we have something to split in the beginning and `begin` can not be 0.
            assert begin > 0;

            //       join3
            //        / \
            //     join2 \
            //      / \   \
            //   join1 \   \
            //    / \   \   \
            //  T0   T1  T2  T3
            //           ^----^

            // We have to push down T2 and T3.
            // Also may be T1, but this does not change much the logic.

            // - Add join3 to W,
            // - replace left branch in join3 with T2,
            // - replace right branch in join2 with W:

            //     join2
            //      / \
            //   join1 \
            //    / \   \
            //  T0   T1  W

            //  W:      join3
            //           / \
            //         T2   T3

            GridSqlJoin beginJoin = findJoin(qrym, begin);
            GridSqlJoin afterBeginJoin = findJoin(qrym, begin + 1);
            GridSqlJoin endJoin = findJoin(qrym, end);

            wrapSelect.from(endJoin);
            afterBeginJoin.leftTable(beginJoin.rightTable());
            beginJoin.rightTable(wrapAlias);
        }
        else if (begin == 0) {
            //  From the first model to some middle one.

            //       join3
            //        / \
            //     join2 \
            //      / \   \
            //   join1 \   \
            //    / \   \   \
            //  T0   T1  T2  T3
            //  ^-----^


            //     join3
            //      / \
            //   join2 \
            //    / \   \
            //   W  T2  T3

            //  W:    join1
            //         / \
            //       T0   T1

            GridSqlJoin endJoin = findJoin(qrym, end);
            GridSqlJoin afterEndJoin = findJoin(qrym, end + 1);

            wrapSelect.from(endJoin);
            afterEndJoin.leftTable(wrapAlias);
        }
        else {
            //  Push down some middle range.

            //       join3
            //        / \
            //     join2 \
            //      / \   \
            //   join1 \   \
            //    / \   \   \
            //  T0   T1  T2  T3
            //       ^----^


            //        join3
            //         / \
            //      join1 \
            //       / \   \
            //     T0   W   T3

            //  W:      join2
            //           / \
            //         T1   T2

            GridSqlJoin beginJoin = findJoin(qrym, begin);
            GridSqlJoin afterBeginJoin = findJoin(qrym, begin + 1);
            GridSqlJoin endJoin = findJoin(qrym, end);
            GridSqlJoin afterEndJoin = findJoin(qrym, end + 1);

            wrapSelect.from(endJoin);
            afterEndJoin.leftTable(beginJoin);
            afterBeginJoin.leftTable(beginJoin.rightTable());
            beginJoin.rightTable(wrapAlias);
        }

        // Get the original SELECT.
        GridSqlSelect select = qrym.ast();
        GridSqlAst from = select.from();

        // Push down related ON conditions for all the related joins.
        while (from instanceof GridSqlJoin) {
            pushDownColumnsInExpression(tblAliases, cols, wrapAlias, from, ON_CHILD);

            from = from.child(LEFT_TABLE_CHILD);
        }
    }

    /**
     * @param tblAliases Table aliases for push down.
     * @param cols Columns with generated aliases.
     * @param wrapAlias Alias of the wrap query.
     * @param select The original select.
     */
    private void pushDownSelectColumns(
        Set<GridSqlAlias> tblAliases,
        Map<String,GridSqlAlias> cols,
        GridSqlAlias wrapAlias,
        GridSqlSelect select
    ) {
        for (int i = 0; i < select.allColumns(); i++) {
            GridSqlAst expr = select.column(i);

            if (expr instanceof GridSqlColumn) {
                // If this is a column with no expression and without alias, we need replace it with an alias,
                // because in the wrapQuery we will generate unique aliases for all the columns to avoid duplicates
                // and all the columns in the will be replaced.
                expr = alias(((GridSqlColumn)expr).columnName(), expr);

                select.setColumn(i, expr);
            }

            for (int c = 0; c < expr.size(); c++)
                pushDownColumnsInExpression(tblAliases, cols, wrapAlias, expr, c);
        }
    }

    /**
     * @param tblAliases Table aliases to push down.
     * @param cols Columns with generated aliases.
     * @param wrapAlias Alias of the wrap query.
     * @param prnt Parent expression.
     * @param childIdx Child index.
     */
    @SuppressWarnings("SuspiciousMethodCalls")
    private void pushDownColumnsInExpression(
        Set<GridSqlAlias> tblAliases,
        Map<String,GridSqlAlias> cols,
        GridSqlAlias wrapAlias,
        GridSqlAst prnt,
        int childIdx
    ) {
        GridSqlAst child = prnt.child(childIdx);

        if (child instanceof GridSqlColumn)
            pushDownColumn(tblAliases, cols, wrapAlias, prnt, childIdx);
        else {
            for (int i = 0; i < child.size(); i++)
                pushDownColumnsInExpression(tblAliases, cols, wrapAlias, child, i);
        }
    }

    /**
     * @param tblAliases Table aliases for push down.
     * @param cols Columns with generated aliases.
     * @param wrapAlias Alias of the wrap query.
     * @param prnt Parent element.
     * @param childIdx Child index.
     */
    private void pushDownColumn(
        Set<GridSqlAlias> tblAliases,
        Map<String,GridSqlAlias> cols,
        GridSqlAlias wrapAlias,
        GridSqlAst prnt,
        int childIdx
    ) {
        GridSqlColumn col = prnt.child(childIdx);

        // It must always be unique table alias.
        GridSqlAlias tblAlias = (GridSqlAlias)col.expressionInFrom();

        assert tblAlias != null; // The query is normalized.

        if (!tblAliases.contains(tblAlias))
            return;

        GridSqlType resType = col.resultType();
        String uniqueColAlias = uniqueColumnAlias(col);
        GridSqlAlias colAlias = cols.get(uniqueColAlias);

        if (colAlias == null) {
            colAlias = alias(uniqueColAlias, col);

            // We have this map to avoid column duplicates in wrap query.
            cols.put(uniqueColAlias, colAlias);
        }

        col = column(uniqueColAlias);
        // col.tableAlias(wrapAlias.alias());
        col.expressionInFrom(wrapAlias);
        col.resultType(resType);

        prnt.child(childIdx, col);
    }

    /**
     * @param col Column.
     * @return Unique column alias based on generated unique table alias.
     */
    private String uniqueColumnAlias(GridSqlColumn col) {
        GridSqlAlias uniqueTblAlias = (GridSqlAlias)col.expressionInFrom();

        return uniqueTblAlias.alias() + "__" + col.columnName();
    }

    /**
     * @param tblAliases Table aliases for push down.
     * @param cols Columns with generated aliases.
     * @param wrapAlias Alias of the wrap query.
     * @param select The original select.
     */
    private void pushDownWhereConditions(
        Set<GridSqlAlias> tblAliases,
        Map<String,GridSqlAlias> cols,
        GridSqlAlias wrapAlias,
        GridSqlSelect select
    ) {
        if (select.where() == null)
            return;

        GridSqlSelect wrapSelect = GridSqlAlias.<GridSqlSubquery>unwrap(wrapAlias).subquery();

        List<AndCondition> andConditions = new ArrayList<>();

        collectAndConditions(andConditions, select, WHERE_CHILD);

        for (int i = 0; i < andConditions.size(); i++) {
            AndCondition c = andConditions.get(i);
            GridSqlAst condition = c.ast();

            if (isAllRelatedToTables(tblAliases, condition)) {
                if (!isTrue(condition)) {
                    // Replace the original condition with `true` and move it to the wrap query.
                    c.prnt.child(c.childIdx, TRUE);
                    wrapSelect.whereAnd(condition);
                }
            }
            else
                pushDownColumnsInExpression(tblAliases, cols, wrapAlias, c.prnt, c.childIdx);
        }
    }

    /**
     * @param expr Expression.
     * @return {@code true} If this expression represents a constant value `TRUE`.
     */
    private static boolean isTrue(GridSqlAst expr) {
        return expr instanceof GridSqlConst && ((GridSqlConst)expr).value() == TRUE.value();
    }

    /**
     * @param tblAliases Table aliases for push down.
     * @param ast AST.
     * @return {@code true} If all the columns in the given expression are related to the given tables.
     */
    @SuppressWarnings("SuspiciousMethodCalls")
    private boolean isAllRelatedToTables(Set<GridSqlAlias> tblAliases, GridSqlAst ast) {
        if (ast instanceof GridSqlColumn) {
            GridSqlColumn col = (GridSqlColumn)ast;

            if (!tblAliases.contains(col.expressionInFrom()))
                return false;
        }
        else {
            for (int i = 0; i < ast.size(); i++) {
                if (!isAllRelatedToTables(tblAliases, ast.child(i)))
                    return false;
            }
        }

        return true;
    }

    /**
     * @param andConditions Conditions in AND.
     * @param prnt Parent Parent element.
     * @param childIdx Child index.
     */
    private void collectAndConditions(List<AndCondition> andConditions, GridSqlAst prnt, int childIdx) {
        GridSqlAst child = prnt.child(childIdx);

        if (child instanceof GridSqlOperation) {
            GridSqlOperation op = (GridSqlOperation)child;

            if (op.operationType() == GridSqlOperationType.AND) {
                collectAndConditions(andConditions, op, 0);
                collectAndConditions(andConditions, op, 1);

                return;
            }
        }

        if (!isTrue(child))
            andConditions.add(new AndCondition(prnt, childIdx));
    }

    /**
     * @return New identity hash set.
     */
    private static <X> Set<X> newIdentityHashSet() {
        return Collections.newSetFromMap(new IdentityHashMap<X,Boolean>());
    }

    /**
     * @param qrym Query model for the SELECT.
     * @param idx Index of the child model for which we need to find a respective JOIN element.
     * @return JOIN.
     */
    private static GridSqlJoin findJoin(QueryModel qrym, int idx) {
        assert qrym.type == Type.SELECT: qrym.type;
        assert qrym.size() > 1; // It must be at least one join with at least two child tables.
        assert idx < qrym.size(): idx;

        //     join2
        //      / \
        //   join1 \
        //    / \   \
        //  T0   T1  T2

        // If we need to find JOIN for T0, it is the same as for T1.
        if (idx == 0)
            idx = 1;

        GridSqlJoin join = (GridSqlJoin)qrym.<GridSqlSelect>ast().from();

        for (int i = qrym.size() - 1; i > idx; i--)
            join = (GridSqlJoin)join.leftTable();

        return join;
    }

    /**
     * @param qrym Query model.
     */
    private void splitQueryModel(QueryModel qrym) {
        switch (qrym.type) {
            case SELECT:
                if (qrym.needSplit) {
                    splitSelect(qrym.prnt, qrym.childIdx);

                    break;
                }

                // Intentional fallthrough to go deeper.
            case UNION:
                for (int i = 0; i < qrym.size(); i++)
                    splitQueryModel(qrym.get(i));

                break;

            default:
                throw new IllegalStateException("Type: " + qrym.type);
        }
    }

    /**
     * @param qrym Query model.
     */
    private void analyzeQueryModel(QueryModel qrym) {
        if (!qrym.isQuery())
            return;

        // Process all the children at the beginning: depth first analysis.
        for (int i = 0; i < qrym.size(); i++) {
            QueryModel child = qrym.get(i);

            analyzeQueryModel(child);

            // Pull up information about the splitting child.
            if (child.needSplit || child.needSplitChild)
                qrym.needSplitChild = true; // We have a child to split.
        }

        if (qrym.type == Type.SELECT) {
            // We may need to split the SELECT only if it has no splittable children,
            // because only the downmost child can be split, the parents will be the part of
            // the reduce query.
            if (!qrym.needSplitChild)
                qrym.needSplit = needSplitSelect(qrym.<GridSqlSelect>ast()); // Only SELECT can have this flag in true.
        }
        else if (qrym.type == Type.UNION) {
            // If it is not a UNION ALL, then we have to split because otherwise we can produce duplicates or
            // wrong results for UNION DISTINCT, EXCEPT, INTERSECT queries.
            if (!qrym.needSplitChild && (!qrym.unionAll || hasOffsetLimit(qrym.<GridSqlUnion>ast())))
                qrym.needSplitChild = true;

            // If we have to split some child SELECT in this UNION, then we have to enforce split
            // for all other united selects, because this UNION has to be a part of the reduce query,
            // thus each SELECT needs to have a reduce part for this UNION, but the whole SELECT can not
            // be a reduce part (usually).
            if (qrym.needSplitChild) {
                for (int i = 0; i < qrym.size(); i++) {
                    QueryModel child = qrym.get(i);

                    assert child.type == Type.SELECT : child.type;

                    if (!child.needSplitChild && !child.needSplit)
                        child.needSplit = true;
                }
            }
        }
        else
            throw new IllegalStateException("Type: " + qrym.type);
    }

    /**
     * @param prntModel Parent model.
     * @param prnt Parent AST element.
     * @param childIdx Child index.
     * @param uniqueAlias Unique parent alias of the current element.
     */
    private void buildQueryModel(QueryModel prntModel, GridSqlAst prnt, int childIdx, GridSqlAlias uniqueAlias) {
        GridSqlAst child = prnt.child(childIdx);

        assert child != null;

        if (child instanceof GridSqlSelect) {
            QueryModel model = new QueryModel(Type.SELECT, prnt, childIdx, uniqueAlias);

            prntModel.add(model);

            buildQueryModel(model, child, FROM_CHILD, null);
        }
        else if (child instanceof GridSqlUnion) {
            QueryModel model;

            // We will collect all the selects into a single UNION model.
            if (prntModel.type == Type.UNION)
                model = prntModel;
            else {
                model = new QueryModel(Type.UNION, prnt, childIdx, uniqueAlias);

                prntModel.add(model);
            }

            if (((GridSqlUnion)child).unionType() != SelectUnion.UNION_ALL)
                model.unionAll = false;

            buildQueryModel(model, child, LEFT_CHILD, null);
            buildQueryModel(model, child, RIGHT_CHILD, null);
        }
        else {
            // Here we must be in FROM clause of the SELECT.
            assert prntModel.type == Type.SELECT : prntModel.type;

            if (child instanceof GridSqlAlias)
                buildQueryModel(prntModel, child, 0, (GridSqlAlias)child);
            else if (child instanceof GridSqlJoin) {
                buildQueryModel(prntModel, child, LEFT_TABLE_CHILD, uniqueAlias);
                buildQueryModel(prntModel, child, RIGHT_TABLE_CHILD, uniqueAlias);
            }
            else {
                // Here we must be inside of generated unique alias for FROM clause element.
                assert prnt == uniqueAlias: prnt.getClass();

                if (child instanceof GridSqlTable)
                    prntModel.add(new QueryModel(Type.TABLE, prnt, childIdx, uniqueAlias));
                else if (child instanceof GridSqlSubquery)
                    buildQueryModel(prntModel, child, 0, uniqueAlias);
                else if (child instanceof GridSqlFunction)
                    prntModel.add(new QueryModel(Type.FUNCTION, prnt, childIdx, uniqueAlias));
                else
                    throw new IllegalStateException("Unknown child type: " + child.getClass());
            }
        }
    }

    /**
     * @param qry Query.
     * @return {@code true} If we have OFFSET LIMIT.
     */
    private static boolean hasOffsetLimit(GridSqlQuery qry) {
        return qry.limit() != null || qry.offset() != null;
    }

    /**
     * @param select Select to check.
     * @return {@code true} If we need to split this select.
     */
    private boolean needSplitSelect(GridSqlSelect select) {
        if (select.distinct())
            return true;

        if (hasOffsetLimit(select))
            return true;

        if (collocatedGrpBy)
            return false;

        if (select.groupColumns() != null)
            return true;

        for (int i = 0; i < select.allColumns(); i++) {
            if (hasAggregates(select.column(i)))
                return true;
        }

        return false;
    }

    /**
     * !!! Notice that here we will modify the original query AST in this method.
     *
     * @param prnt Parent AST element.
     * @param childIdx Index of child select.
     */
    private void splitSelect(
        final GridSqlAst prnt,
        final int childIdx
    ) {
        if (++splitId > 99)
            throw new CacheException("Too complex query to process.");

        final GridSqlSelect mapQry = prnt.child(childIdx);

        final int visibleCols = mapQry.visibleColumns();

        List<GridSqlAst> rdcExps = new ArrayList<>(visibleCols);
        List<GridSqlAst> mapExps = new ArrayList<>(mapQry.allColumns());

        mapExps.addAll(mapQry.columns(false));

        Set<String> colNames = new HashSet<>();
        final int havingCol = mapQry.havingColumn();

        boolean distinctAggregateFound = false;

        if (!collocatedGrpBy) {
            for (int i = 0, len = mapExps.size(); i < len; i++)
                distinctAggregateFound |= hasDistinctAggregates(mapExps.get(i));
        }

        boolean aggregateFound = distinctAggregateFound;

        // Split all select expressions into map-reduce parts.
        for (int i = 0, len = mapExps.size(); i < len; i++) // Remember len because mapExps list can grow.
            aggregateFound |= splitSelectExpression(mapExps, rdcExps, colNames, i, collocatedGrpBy, i == havingCol,
                distinctAggregateFound);

        assert !(collocatedGrpBy && aggregateFound); // We do not split aggregates when collocatedGrpBy is true.

        // Create reduce query AST. Use unique merge table for this split.
        GridSqlSelect rdcQry = new GridSqlSelect().from(mergeTable(splitId));

        // -- SELECT
        mapQry.clearColumns();

        for (GridSqlAst exp : mapExps) // Add all map expressions as visible.
            mapQry.addColumn(exp, true);

        for (int i = 0; i < visibleCols; i++) // Add visible reduce columns.
            rdcQry.addColumn(rdcExps.get(i), true);

        for (int i = visibleCols; i < rdcExps.size(); i++) // Add invisible reduce columns (HAVING).
            rdcQry.addColumn(rdcExps.get(i), false);

        for (int i = rdcExps.size(); i < mapExps.size(); i++)  // Add all extra map columns as invisible reduce columns.
            rdcQry.addColumn(column(((GridSqlAlias)mapExps.get(i)).alias()), false);

        // -- FROM WHERE: do nothing

        // -- GROUP BY
        if (mapQry.groupColumns() != null && !collocatedGrpBy) {
            rdcQry.groupColumns(mapQry.groupColumns());

            // Grouping with distinct aggregates cannot be performed on map phase
            if (distinctAggregateFound)
                mapQry.groupColumns(null);
        }

        // -- HAVING
        if (havingCol >= 0 && !collocatedGrpBy) {
            // TODO IGNITE-1140 - Find aggregate functions in HAVING clause or rewrite query to put all aggregates to SELECT clause.
            // We need to find HAVING column in reduce query.
            for (int i = visibleCols; i < rdcQry.allColumns(); i++) {
                GridSqlAst c = rdcQry.column(i);

                if (c instanceof GridSqlAlias && HAVING_COLUMN.equals(((GridSqlAlias)c).alias())) {
                    rdcQry.havingColumn(i);

                    break;
                }
            }

            mapQry.havingColumn(-1);
        }

        // -- ORDER BY
        if (!mapQry.sort().isEmpty()) {
            for (GridSqlSortColumn sortCol : mapQry.sort())
                rdcQry.addSort(sortCol);

            // If collocatedGrpBy is true, then aggregateFound is always false.
            if (aggregateFound) // Ordering over aggregates does not make sense.
                mapQry.clearSort(); // Otherwise map sort will be used by offset-limit.
            // TODO IGNITE-1141 - Check if sorting is done over aggregated expression, otherwise we can sort and use offset-limit.
        }

        // -- LIMIT
        if (mapQry.limit() != null) {
            rdcQry.limit(mapQry.limit());

            // Will keep limits on map side when collocatedGrpBy is true,
            // because in this case aggregateFound is always false.
            if (aggregateFound)
                mapQry.limit(null);
        }

        // -- OFFSET
        if (mapQry.offset() != null) {
            rdcQry.offset(mapQry.offset());

            if (mapQry.limit() != null) // LIMIT off + lim
                mapQry.limit(op(GridSqlOperationType.PLUS, mapQry.offset(), mapQry.limit()));

            mapQry.offset(null);
        }

        // -- DISTINCT
        if (mapQry.distinct()) {
            mapQry.distinct(!aggregateFound && mapQry.groupColumns() == null && mapQry.havingColumn() < 0);
            rdcQry.distinct(true);
        }

        // Replace the given select with generated reduce query in the parent.
        prnt.child(childIdx, rdcQry);

        // Setup resulting map query.
        GridCacheSqlQuery map = new GridCacheSqlQuery(mapQry.getSQL());

        setupParameters(map, mapQry, params);
        map.columns(collectColumns(mapExps));
        map.sortColumns(mapQry.sort());
        map.partitioned(hasPartitionedTables(mapQry));

        mapSqlQrys.add(map);
    }

    /**
     * @param ast Map query AST.
     * @return {@code true} If the given AST has partitioned tables.
     */
    private static boolean hasPartitionedTables(GridSqlAst ast) {
        if (ast instanceof GridSqlTable)
            return ((GridSqlTable)ast).dataTable().isPartitioned();

        for (int i = 0; i < ast.size(); i++) {
            if (hasPartitionedTables(ast.child(i)))
                return true;
        }

        return false;
    }

    /**
     * @param sqlQry Query.
     * @param qryAst Select AST.
     * @param params All parameters.
     */
    private static void setupParameters(GridCacheSqlQuery sqlQry, GridSqlQuery qryAst, Object[] params) {
        IntArray paramIdxs = new IntArray(params.length);

        params = findParams(qryAst, params, new ArrayList<>(params.length), paramIdxs).toArray();

        sqlQry.parameters(params, toArray(paramIdxs));
    }

    /**
     * @param cols Columns from SELECT clause.
     * @return Map of columns with types.
     */
    private LinkedHashMap<String,?> collectColumns(List<GridSqlAst> cols) {
        LinkedHashMap<String, GridSqlType> res = new LinkedHashMap<>(cols.size(), 1f, false);

        for (int i = 0; i < cols.size(); i++) {
            GridSqlAst col = cols.get(i);
            GridSqlType t = col.resultType();

            if (t == null)
                throw new NullPointerException("Column type: " + col);

            if (t == GridSqlType.UNKNOWN)
                throw new IllegalStateException("Unknown type: " + col);

            String alias;

            if (col instanceof GridSqlAlias)
                alias = ((GridSqlAlias)col).alias();
            else
                alias = columnName(i);

            if (res.put(alias, t) != null)
                throw new IllegalStateException("Alias already exists: " + alias);
        }

        return res;
    }

    /**
     * @param prepared Prepared command.
     * @param useOptimizedSubqry Use optimized subqueries order for table filters.
     * @return Parsed SQL query AST.
     */
    private static GridSqlQuery parse(Prepared prepared, boolean useOptimizedSubqry) {
        return (GridSqlQuery)new GridSqlQueryParser(useOptimizedSubqry).parse(prepared);
    }

    /**
     * @param h2 Indexing.
     * @param c Connection.
     * @param qry Parsed query.
     * @param params Query parameters.
     * @param enforceJoinOrder Enforce join order.
     * @return Optimized prepared command.
     * @throws SQLException If failed.
     * @throws IgniteCheckedException If failed.
     */
    private static Prepared optimize(
        IgniteH2Indexing h2,
        Connection c,
        String qry,
        Object[] params,
        boolean distributedJoins,
        boolean enforceJoinOrder
    ) throws SQLException, IgniteCheckedException {
        setupConnection(c, distributedJoins, enforceJoinOrder);

        try (PreparedStatement s = c.prepareStatement(qry)) {
            h2.bindParameters(s, F.asList(params));

            return prepared(s);
        }
    }

    /**
     * @param qry Query.
     */
    private void normalizeQuery(GridSqlQuery qry) {
        if (qry instanceof GridSqlUnion) {
            GridSqlUnion union = (GridSqlUnion)qry;

            normalizeQuery(union.left());
            normalizeQuery(union.right());
        }
        else {
            GridSqlSelect select = (GridSqlSelect)qry;

            // Normalize FROM first to update column aliases after.
            normalizeFrom(select, FROM_CHILD, false);

            List<GridSqlAst> cols = select.columns(false);

            for (int i = 0; i < cols.size(); i++)
                normalizeExpression(select, childIndexForColumn(i));

            normalizeExpression(select, WHERE_CHILD);

            // ORDER BY and HAVING are in SELECT expressions.
        }

        normalizeExpression(qry, OFFSET_CHILD);
        normalizeExpression(qry, LIMIT_CHILD);
    }

    /**
     * @param prnt Table parent element.
     * @param childIdx Child index for the table or alias containing the table.
     * @return Generated alias.
     */
    private GridSqlAlias generateUniqueAlias(GridSqlAst prnt, int childIdx) {
        GridSqlAst child = prnt.child(childIdx);
        GridSqlAst tbl = GridSqlAlias.unwrap(child);

        assert tbl instanceof GridSqlTable || tbl instanceof GridSqlSubquery ||
            tbl instanceof GridSqlFunction: tbl.getClass();

        String uniqueAlias = nextUniqueTableAlias(tbl != child ? ((GridSqlAlias)child).alias() : null);

        GridSqlAlias uniqueAliasAst = new GridSqlAlias(uniqueAlias, tbl);

        uniqueFromAliases.put(tbl, uniqueAliasAst);

        // Replace the child in the parent.
        prnt.child(childIdx, uniqueAliasAst);

        return uniqueAliasAst;
    }

    /**
     * @param origAlias Original alias.
     * @return Generated unique alias.
     */
    private String nextUniqueTableAlias(String origAlias) {
        String uniqueAlias = UNIQUE_TABLE_ALIAS_SUFFIX + nextTblAliasId++;

        if (origAlias != null) // Prepend with the original table alias for better plan readability.
            uniqueAlias = origAlias + uniqueAlias;

        return uniqueAlias;
    }


    /**
     * @param prnt Parent element.
     * @param childIdx Child index.
     * @param prntAlias If the parent is {@link GridSqlAlias}.
     */
    private void normalizeFrom(GridSqlAst prnt, int childIdx, boolean prntAlias) {
        GridSqlElement from = prnt.child(childIdx);

        if (from instanceof GridSqlTable) {
            GridSqlTable tbl = (GridSqlTable)from;

            String schema = tbl.schema();

            boolean addSchema = tbls == null;

            if (tbls != null)
                addSchema = tbls.add(tbl.dataTable().identifier());

            if (addSchema && schema != null && schemas != null)
                schemas.add(schema);

            // In case of alias parent we need to replace the alias itself.
            if (!prntAlias)
                generateUniqueAlias(prnt, childIdx);
        }
        else if (from instanceof GridSqlAlias) {
            // Replace current alias with generated unique alias.
            normalizeFrom(from, 0, true);
            generateUniqueAlias(prnt, childIdx);
        }
        else if (from instanceof GridSqlSubquery) {
            // We do not need to wrap simple functional subqueries into filtering function,
            // because we can not have any other tables than Ignite (which are already filtered)
            // and functions we have to filter explicitly as well.
            normalizeQuery(((GridSqlSubquery)from).subquery());

            if (!prntAlias) // H2 generates aliases for subqueries in FROM clause.
                throw new IllegalStateException("No alias for subquery: " + from.getSQL());
        }
        else if (from instanceof GridSqlJoin) {
            // Left and right.
            normalizeFrom(from, 0, false);
            normalizeFrom(from, 1, false);

            // Join condition (after ON keyword).
            normalizeExpression(from, 2);
        }
        else if (from instanceof GridSqlFunction) {
            // TODO generate filtering function around the given function
            // TODO SYSTEM_RANGE is a special case, it can not be wrapped

            // In case of alias parent we need to replace the alias itself.
            if (!prntAlias)
                generateUniqueAlias(prnt, childIdx);
        }
        else
            throw new IllegalStateException(from.getClass().getName() + " : " + from.getSQL());
    }

    /**
     * @param prnt Parent element.
     * @param childIdx Child index.
     */
    private void normalizeExpression(GridSqlAst prnt, int childIdx) {
        GridSqlAst el = prnt.child(childIdx);

        if (el instanceof GridSqlAlias ||
            el instanceof GridSqlOperation ||
            el instanceof GridSqlFunction ||
            el instanceof GridSqlArray) {
            for (int i = 0; i < el.size(); i++)
                normalizeExpression(el, i);
        }
        else if (el instanceof GridSqlSubquery)
            normalizeQuery(((GridSqlSubquery)el).subquery());
        else if (el instanceof GridSqlColumn) {
            GridSqlColumn col = (GridSqlColumn)el;
            GridSqlAst tbl = GridSqlAlias.unwrap(col.expressionInFrom());

            // Change table alias part of the column to the generated unique table alias.
            GridSqlAlias uniqueAlias = uniqueFromAliases.get(tbl);

            // Unique aliases must be generated for all the table filters already.
            assert uniqueAlias != null: childIdx + "\n" + prnt.getSQL();

            col.tableAlias(uniqueAlias.alias());
            col.expressionInFrom(uniqueAlias);
        }
        else if (el instanceof GridSqlParameter ||
            el instanceof GridSqlPlaceholder ||
            el instanceof GridSqlConst) {
            // No-op for simple expressions.
        }
        else
            throw new IllegalStateException(el + ": " + el.getClass());
    }

    /**
     * @param qry Select.
     * @param params Parameters.
     * @param target Extracted parameters.
     * @param paramIdxs Parameter indexes.
     * @return Extracted parameters list.
     */
    private static List<Object> findParams(GridSqlQuery qry, Object[] params, ArrayList<Object> target,
        IntArray paramIdxs) {
        if (qry instanceof GridSqlSelect)
            return findParams((GridSqlSelect)qry, params, target, paramIdxs);

        GridSqlUnion union = (GridSqlUnion)qry;

        findParams(union.left(), params, target, paramIdxs);
        findParams(union.right(), params, target, paramIdxs);

        findParams(qry.limit(), params, target, paramIdxs);
        findParams(qry.offset(), params, target, paramIdxs);

        return target;
    }

    /**
     * @param select Select.
     * @param params Parameters.
     * @param target Extracted parameters.
     * @param paramIdxs Parameter indexes.
     * @return Extracted parameters list.
     */
    private static List<Object> findParams(
        GridSqlSelect select,
        Object[] params,
        ArrayList<Object> target,
        IntArray paramIdxs
    ) {
        if (params.length == 0)
            return target;

        for (GridSqlAst el : select.columns(false))
            findParams(el, params, target, paramIdxs);

        findParams(select.from(), params, target, paramIdxs);
        findParams(select.where(), params, target, paramIdxs);

        // Don't search in GROUP BY and HAVING since they expected to be in select list.

        findParams(select.limit(), params, target, paramIdxs);
        findParams(select.offset(), params, target, paramIdxs);

        return target;
    }

    /**
     * @param el Element.
     * @param params Parameters.
     * @param target Extracted parameters.
     * @param paramIdxs Parameter indexes.
     */
    private static void findParams(@Nullable GridSqlAst el, Object[] params, ArrayList<Object> target,
        IntArray paramIdxs) {
        if (el == null)
            return;

        if (el instanceof GridSqlParameter) {
            // H2 Supports queries like "select ?5" but first 4 non-existing parameters are need to be set to any value.
            // Here we will set them to NULL.
            final int idx = ((GridSqlParameter)el).index();

            while (target.size() < idx)
                target.add(null);

            if (params.length <= idx)
                throw new IgniteException("Invalid number of query parameters. " +
                    "Cannot find " + idx + " parameter.");

            Object param = params[idx];

            if (idx == target.size())
                target.add(param);
            else
                target.set(idx, param);

            paramIdxs.add(idx);
        }
        else if (el instanceof GridSqlSubquery)
            findParams(((GridSqlSubquery)el).subquery(), params, target, paramIdxs);
        else {
            for (int i = 0; i < el.size(); i++)
                findParams((GridSqlAst)el.child(i), params, target, paramIdxs);
        }
    }

    /**
     * @param mapSelect Selects for map query.
     * @param rdcSelect Selects for reduce query.
     * @param colNames Set of unique top level column names.
     * @param idx Index.
     * @param collocatedGrpBy If it is a collocated GROUP BY query.
     * @param isHaving If it is a HAVING expression.
     * @param hasDistinctAggregate If query has distinct aggregate expression.
     * @return {@code true} If aggregate was found.
     */
    private boolean splitSelectExpression(
        List<GridSqlAst> mapSelect,
        List<GridSqlAst> rdcSelect,
        Set<String> colNames,
        final int idx,
        boolean collocatedGrpBy,
        boolean isHaving,
        boolean hasDistinctAggregate
    ) {
        GridSqlAst el = mapSelect.get(idx);
        GridSqlAlias alias = null;

        boolean aggregateFound = false;

        if (el instanceof GridSqlAlias) { // Unwrap from alias.
            alias = (GridSqlAlias)el;
            el = alias.child();
        }

        if (!collocatedGrpBy && hasAggregates(el)) {
            aggregateFound = true;

            if (alias == null)
                alias = alias(isHaving ? HAVING_COLUMN : columnName(idx), el);

            // We can update original alias here as well since it will be dropped from mapSelect.
            splitAggregates(alias, 0, mapSelect, idx, hasDistinctAggregate, true);

            set(rdcSelect, idx, alias);
        }
        else {
            String mapColAlias = isHaving ? HAVING_COLUMN : columnName(idx);
            String rdcColAlias;

            if (alias == null)  // Original column name for reduce column.
                rdcColAlias = el instanceof GridSqlColumn ? ((GridSqlColumn)el).columnName() : mapColAlias;
            else // Set initial alias for reduce column.
                rdcColAlias = alias.alias();

            // Always wrap map column into generated alias.
            mapSelect.set(idx, alias(mapColAlias, el)); // `el` is known not to be an alias.

            // SELECT __C0 AS original_alias
            GridSqlElement rdcEl = column(mapColAlias);

            if (colNames.add(rdcColAlias)) // To handle column name duplication (usually wildcard for few tables).
                rdcEl = alias(rdcColAlias, rdcEl);

            set(rdcSelect, idx, rdcEl);
        }

        return aggregateFound;
    }

    /**
     * @param list List.
     * @param idx Index.
     * @param item Element.
     */
    private static <Z> void set(List<Z> list, int idx, Z item) {
        assert list.size() == idx;
        list.add(item);
    }

    /**
     * @param el Expression.
     * @return {@code true} If expression contains aggregates.
     */
    private static boolean hasAggregates(GridSqlAst el) {
        if (el instanceof GridSqlAggregateFunction)
            return true;

        for (int i = 0; i < el.size(); i++) {
            if (hasAggregates(el.child(i)))
                return true;
        }

        return false;
    }

    /**
     * Lookup for distinct aggregates.
     * Note, DISTINCT make no sense for MIN and MAX aggregates, so its will be ignored.
     *
     * @param el Expression.
     * @return {@code true} If expression contains distinct aggregates.
     */
    private static boolean hasDistinctAggregates(GridSqlAst el) {
        if (el instanceof GridSqlAggregateFunction) {
            GridSqlFunctionType type = ((GridSqlAggregateFunction)el).type();

            return ((GridSqlAggregateFunction)el).distinct() && type != MIN && type != MAX;
        }

        for (int i = 0; i < el.size(); i++) {
            if (hasDistinctAggregates(el.child(i)))
                return true;
        }

        return false;
    }

    /**
     * @param parentExpr Parent expression.
     * @param childIdx Child index to try to split.
     * @param mapSelect List of expressions in map SELECT clause.
     * @param exprIdx Index of the original expression in map SELECT clause.
     * @param hasDistinctAggregate If query has distinct aggregate expression.
     * @param first If the first aggregate is already found in this expression.
     * @return {@code true} If the first aggregate is already found.
     */
    private boolean splitAggregates(
        final GridSqlAst parentExpr,
        final int childIdx,
        final List<GridSqlAst> mapSelect,
        final int exprIdx,
        boolean hasDistinctAggregate,
        boolean first
    ) {
        GridSqlAst el = parentExpr.child(childIdx);

        if (el instanceof GridSqlAggregateFunction) {
            splitAggregate(parentExpr, childIdx, mapSelect, exprIdx, hasDistinctAggregate, first);

            return true;
        }

        for (int i = 0; i < el.size(); i++) {
            if (splitAggregates(el, i, mapSelect, exprIdx, hasDistinctAggregate, first))
                first = false;
        }

        return !first;
    }

    /**
     * @param parentExpr Parent expression.
     * @param aggIdx Index of the aggregate to split in this expression.
     * @param mapSelect List of expressions in map SELECT clause.
     * @param exprIdx Index of the original expression in map SELECT clause.
     * @param hasDistinctAggregate If query has distinct aggregate expression.
     * @param first If this is the first aggregate found in this expression.
     */
    private void splitAggregate(
        GridSqlAst parentExpr,
        int aggIdx,
        List<GridSqlAst> mapSelect,
        int exprIdx,
        boolean hasDistinctAggregate,
        boolean first
    ) {
        GridSqlAggregateFunction agg = parentExpr.child(aggIdx);

        assert agg.resultType() != null;

        GridSqlElement mapAgg, rdcAgg;

        // Create stubbed map alias to fill it with correct expression later.
        GridSqlAlias mapAggAlias = alias(columnName(first ? exprIdx : mapSelect.size()), EMPTY);

        // Replace original expression if it is the first aggregate in expression or add to the end.
        if (first)
            mapSelect.set(exprIdx, mapAggAlias);
        else
            mapSelect.add(mapAggAlias);

        /* Note Distinct aggregate can be performed only on reduce phase, so
           if query contains distinct aggregate then other aggregates must be processed the same way. */
        switch (agg.type()) {
            case AVG: // SUM( AVG(CAST(x AS DOUBLE))*COUNT(x) )/SUM( COUNT(x) )  or  AVG(CAST( x AS DOUBLE))
                if (hasDistinctAggregate) /* and has no collocated group by */ {
                    mapAgg = agg.child();

                    rdcAgg = aggregate(agg.distinct(), agg.type()).resultType(GridSqlType.DOUBLE)
                        .addChild(function(CAST).resultType(GridSqlType.DOUBLE).addChild(column(mapAggAlias.alias())));
                }
                else {
                    //-- COUNT(x) map
                    GridSqlElement cntMapAgg = aggregate(agg.distinct(), COUNT)
                        .resultType(GridSqlType.BIGINT).addChild(agg.child());

                    // Add generated alias to COUNT(x).
                    // Using size as index since COUNT will be added as the last select element to the map query.
                    String cntMapAggAlias = columnName(mapSelect.size());

                    cntMapAgg = alias(cntMapAggAlias, cntMapAgg);

                    mapSelect.add(cntMapAgg);

                    //-- AVG(CAST(x AS DOUBLE)) map
                    mapAgg = aggregate(agg.distinct(), AVG).resultType(GridSqlType.DOUBLE).addChild(
                        function(CAST).resultType(GridSqlType.DOUBLE).addChild(agg.child()));

                    //-- SUM( AVG(x)*COUNT(x) )/SUM( COUNT(x) ) reduce
                    GridSqlElement sumUpRdc = aggregate(false, SUM).addChild(
                        op(GridSqlOperationType.MULTIPLY,
                            column(mapAggAlias.alias()),
                            column(cntMapAggAlias)));

                    GridSqlElement sumDownRdc = aggregate(false, SUM).addChild(column(cntMapAggAlias));

                    rdcAgg = op(GridSqlOperationType.DIVIDE, sumUpRdc, sumDownRdc);
                }

                break;

            case SUM: // SUM( SUM(x) ) or SUM(DISTINCT x)
            case MAX: // MAX( MAX(x) ) or MAX(DISTINCT x)
            case MIN: // MIN( MIN(x) ) or MIN(DISTINCT x)
                if (hasDistinctAggregate) /* and has no collocated group by */ {
                    mapAgg = agg.child();

                    rdcAgg = aggregate(agg.distinct(), agg.type()).addChild(column(mapAggAlias.alias()));
                }
                else {
                    mapAgg = aggregate(agg.distinct(), agg.type()).resultType(agg.resultType()).addChild(agg.child());
                    rdcAgg = aggregate(agg.distinct(), agg.type()).addChild(column(mapAggAlias.alias()));
                }

                break;

            case COUNT_ALL: // CAST(SUM( COUNT(*) ) AS BIGINT)
            case COUNT: // CAST(SUM( COUNT(x) ) AS BIGINT) or CAST(COUNT(DISTINCT x) AS BIGINT)
                if (hasDistinctAggregate) /* and has no collocated group by */ {
                    assert agg.type() == COUNT;

                    mapAgg = agg.child();

                    rdcAgg = aggregate(agg.distinct(), agg.type()).resultType(GridSqlType.BIGINT)
                        .addChild(column(mapAggAlias.alias()));
                }
                else {
                    mapAgg = aggregate(agg.distinct(), agg.type()).resultType(GridSqlType.BIGINT);

                    if (agg.type() == COUNT)
                        mapAgg.addChild(agg.child());

                    rdcAgg = aggregate(false, SUM).addChild(column(mapAggAlias.alias()));
                    rdcAgg = function(CAST).resultType(GridSqlType.BIGINT).addChild(rdcAgg);
                }

                break;

            default:
                throw new IgniteException("Unsupported aggregate: " + agg.type());
        }

        assert !(mapAgg instanceof GridSqlAlias);
        assert mapAgg.resultType() != null;

        // Fill the map alias with aggregate.
        mapAggAlias.child(0, mapAgg);
        mapAggAlias.resultType(mapAgg.resultType());

        // Replace in original expression aggregate with reduce aggregate.
        parentExpr.child(aggIdx, rdcAgg);
    }

    /**
     * @param distinct Distinct.
     * @param type Type.
     * @return Aggregate function.
     */
    private static GridSqlAggregateFunction aggregate(boolean distinct, GridSqlFunctionType type) {
        return new GridSqlAggregateFunction(distinct, type);
    }

    /**
     * @param name Column name.
     * @return Column.
     */
    private static GridSqlColumn column(String name) {
        return new GridSqlColumn(null, null, null, null, name);
    }

    /**
     * @param alias Alias.
     * @param child Child.
     * @return Alias.
     */
    private static GridSqlAlias alias(String alias, GridSqlAst child) {
        GridSqlAlias res = new GridSqlAlias(alias, child);

        res.resultType(child.resultType());

        return res;
    }

    /**
     * @param type Type.
     * @param left Left expression.
     * @param right Right expression.
     * @return Binary operator.
     */
    private static GridSqlOperation op(GridSqlOperationType type, GridSqlAst left, GridSqlAst right) {
        return new GridSqlOperation(type, left, right);
    }

    /**
     * @param type Type.
     * @return Function.
     */
    private static GridSqlFunction function(GridSqlFunctionType type) {
        return new GridSqlFunction(type);
    }

    /**
     * Simplified tree-like model for a query.
     * - SELECT : All the children are list of joined query models in the FROM clause.
     * - UNION  : All the children are united left and right query models.
     * - TABLE and FUNCTION : Never have child models.
     */
    private static final class QueryModel extends ArrayList<QueryModel> {
        /** */
        @GridToStringInclude
        final Type type;

        /** */
        GridSqlAlias uniqueAlias;

        /** */
        GridSqlAst prnt;

        /** */
        int childIdx;

        /** If it is a SELECT and we need to split it. Makes sense only for type SELECT. */
        @GridToStringInclude
        boolean needSplit;

        /** If we have a child SELECT that we should split. */
        @GridToStringInclude
        boolean needSplitChild;

        /** If this is UNION ALL. Makes sense only for type UNION.*/
        boolean unionAll = true;

        /**
         * @param type Type.
         * @param prnt Parent element.
         * @param childIdx Child index.
         * @param uniqueAlias Unique parent alias of the current element.
         *                    May be {@code null} for selects inside of unions or top level queries.
         */
        QueryModel(Type type, GridSqlAst prnt, int childIdx, GridSqlAlias uniqueAlias) {
            this.type = type;
            this.prnt = prnt;
            this.childIdx = childIdx;
            this.uniqueAlias = uniqueAlias;
        }

        /**
         * @return The actual AST element for this model.
         */
        private <X extends GridSqlAst> X ast() {
            return prnt.child(childIdx);
        }

        /**
         * @return {@code true} If this is a SELECT or UNION query model.
         */
        private boolean isQuery() {
            return type == Type.SELECT || type == Type.UNION;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(QueryModel.class, this);
        }
    }

    /**
     * Allowed types for {@link QueryModel}.
     */
    private enum Type {
        SELECT, UNION, TABLE, FUNCTION
    }

    /**
     * Condition in AND.
     */
    private static class AndCondition {
        /** */
        GridSqlAst prnt;

        /** */
        int childIdx;

        /**
         * @param prnt Parent element.
         * @param childIdx Child index.
         */
        AndCondition(GridSqlAst prnt, int childIdx) {
            this.prnt = prnt;
            this.childIdx = childIdx;
        }

        /**
         * @return The actual AST element for this expression.
         */
        private <X extends GridSqlAst> X ast() {
            return prnt.child(childIdx);
        }
    }
}
