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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.query.GridCacheSqlQuery;
import org.apache.ignite.internal.processors.cache.query.GridCacheTwoStepQuery;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.cache.query.QueryTable;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.h2.H2PooledConnection;
import org.apache.ignite.internal.processors.query.h2.H2StatementCache;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.affinity.PartitionExtractor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.opt.QueryContext;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.h2.command.Prepared;
import org.h2.command.dml.Query;
import org.h2.table.Column;

import static org.apache.ignite.internal.processors.query.h2.opt.join.CollocationModel.isCollocated;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlConst.TRUE;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlFunctionType.AVG;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlFunctionType.CAST;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlFunctionType.COUNT;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlFunctionType.GROUP_CONCAT;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlFunctionType.SUM;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlJoin.LEFT_TABLE_CHILD;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlJoin.ON_CHILD;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlJoin.RIGHT_TABLE_CHILD;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlPlaceholder.EMPTY;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlQuery.LIMIT_CHILD;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlQuery.OFFSET_CHILD;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlSelect.FROM_CHILD;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlSelect.WHERE_CHILD;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlSelect.childIndexForColumn;

/**
 * Splits a single SQL query into two step map-reduce query.
 */
@SuppressWarnings("ForLoopReplaceableByForEach")
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
    private static final String EXPR_ALIAS_PREFIX = "__X";

    /** */
    private int nextExprAliasId;

    /** */
    private int nextTblAliasId;

    /** */
    private int splitId = -1; // The first one will be 0.

    /** Query tables. */
    private final Set<QueryTable> tbls = new HashSet<>();

    /** */
    private final Set<String> pushedDownCols = new HashSet<>();

    /** */
    private boolean skipMergeTbl;

    /** */
    private GridCacheSqlQuery rdcSqlQry;

    /** */
    private final List<GridCacheSqlQuery> mapSqlQrys = new ArrayList<>();

    /** */
    private int paramsCnt;

    /** */
    private final boolean collocatedGrpBy;

    /** Whether partition extraction is possible. */
    private final boolean canExtractPartitions;

    /** */
    private final IdentityHashMap<GridSqlAst, GridSqlAlias> uniqueFromAliases = new IdentityHashMap<>();

    /** Partition extractor. */
    private final PartitionExtractor extractor;

    /**
     * @param paramsCnt Parameters count.
     * @param collocatedGrpBy If it is a collocated GROUP BY query.
     * @param distributedJoins Distributed joins flag.
     * @param locSplit Local split flag.
     * @param extractor Partition extractor.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public GridSqlQuerySplitter(
        int paramsCnt,
        boolean collocatedGrpBy,
        boolean distributedJoins,
        boolean locSplit,
        PartitionExtractor extractor
    ) {
        this.paramsCnt = paramsCnt;
        this.collocatedGrpBy = collocatedGrpBy;
        this.extractor = extractor;

        // Partitions *CANNOT* be extracted if:
        // 1) Distributed joins are enabled (https://issues.apache.org/jira/browse/IGNITE-10971)
        // 2) This is a local query with split (https://issues.apache.org/jira/browse/IGNITE-11316)
        canExtractPartitions = !distributedJoins && !locSplit;
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
     * @param conn Connection.
     * @param qry Query.
     * @param originalSql Original SQL query string.
     * @param collocatedGrpBy Whether the query has collocated GROUP BY keys.
     * @param distributedJoins If distributed joins enabled.
     * @param enforceJoinOrder Enforce join order.
     * @param locSplit Whether this is a split for local query.
     * @param idx Indexing.
     * @param paramsCnt Parameters count.
     * @return Two step query.
     * @throws SQLException If failed.
     * @throws IgniteCheckedException If failed.
     */
    public static GridCacheTwoStepQuery split(
        H2PooledConnection conn,
        GridSqlQuery qry,
        String originalSql,
        boolean collocatedGrpBy,
        boolean distributedJoins,
        boolean enforceJoinOrder,
        boolean locSplit,
        IgniteH2Indexing idx,
        int paramsCnt,
        IgniteLogger log
    ) throws SQLException, IgniteCheckedException {
        SplitterContext.set(distributedJoins);

        try {
            return split0(
                conn,
                qry,
                originalSql,
                collocatedGrpBy,
                distributedJoins,
                enforceJoinOrder,
                locSplit,
                idx,
                paramsCnt,
                log
            );
        }
        finally {
            SplitterContext.set(false);
        }
    }

    /**
     * @param conn Connection.
     * @param qry Query.
     * @param originalSql Original SQL query string.
     * @param collocatedGrpBy Whether the query has collocated GROUP BY keys.
     * @param distributedJoins If distributed joins enabled.
     * @param enforceJoinOrder Enforce join order.
     * @param locSplit Whether this is a split for local query.
     * @param idx Indexing.
     * @param paramsCnt Parameters count.
     * @return Two step query.
     * @throws SQLException If failed.
     * @throws IgniteCheckedException If failed.
     */
    private static GridCacheTwoStepQuery split0(
        H2PooledConnection conn,
        GridSqlQuery qry,
        String originalSql,
        boolean collocatedGrpBy,
        boolean distributedJoins,
        boolean enforceJoinOrder,
        boolean locSplit,
        IgniteH2Indexing idx,
        int paramsCnt,
        IgniteLogger log
    ) throws SQLException, IgniteCheckedException {
        final boolean explain = qry.explain();

        qry.explain(false);

        GridSqlQuerySplitter splitter = new GridSqlQuerySplitter(
            paramsCnt,
            collocatedGrpBy,
            distributedJoins,
            locSplit,
            idx.partitionExtractor()
        );

        // Normalization will generate unique aliases for all the table filters in FROM.
        // Also it will collect all tables and schemas from the query.
        splitter.normalizeQuery(qry);

        // Here we will have correct normalized AST with optimized join order.
        // The distributedJoins parameter is ignored because it is not relevant for
        // the REDUCE query optimization.
        qry = GridSqlQueryParser.parseQuery(
            prepare(conn, H2Utils.context(conn), qry.getSQL(), false, enforceJoinOrder),
            true, log);

        // Do the actual query split. We will update the original query AST, need to be careful.
        splitter.splitQuery(qry);

        assert !F.isEmpty(splitter.mapSqlQrys) : "map"; // We must have at least one map query.
        assert splitter.rdcSqlQry != null : "rdc"; // We must have a reduce query.

        // If we have distributed joins, then we have to optimize all MAP side queries
        // to have a correct join order with respect to batched joins and check if we need
        // distributed joins at all.
        if (distributedJoins) {
            boolean allCollocated = true;

            for (GridCacheSqlQuery mapSqlQry : splitter.mapSqlQrys) {
                Prepared prepared0 = prepare(
                    conn,
                    H2Utils.context(conn),
                    mapSqlQry.query(),
                    true,
                    enforceJoinOrder);

                allCollocated &= isCollocated((Query)prepared0);

                mapSqlQry.query(GridSqlQueryParser.parseQuery(prepared0, true, log).getSQL());
            }

            // We do not need distributed joins if all MAP queries are collocated.
            if (allCollocated)
                distributedJoins = false;
        }

        List<Integer> cacheIds = H2Utils.collectCacheIds(idx, null, splitter.tbls);
        boolean mvccEnabled = H2Utils.collectMvccEnabled(idx, cacheIds);
        boolean replicatedOnly = splitter.mapSqlQrys.stream().noneMatch(GridCacheSqlQuery::isPartitioned);

        H2Utils.checkQuery(idx, cacheIds, splitter.tbls);

        // Setup resulting two step query and return it.
        return new GridCacheTwoStepQuery(
            originalSql,
            paramsCnt,
            splitter.tbls,
            splitter.rdcSqlQry,
            splitter.mapSqlQrys,
            splitter.skipMergeTbl,
            explain,
            distributedJoins,
            replicatedOnly,
            splitter.extractor.mergeMapQueries(splitter.mapSqlQrys),
            cacheIds,
            mvccEnabled,
            locSplit
        );
    }

    /**
     * @param qry Optimized and normalized query to split.
     */
    private void splitQuery(GridSqlQuery qry) throws IgniteCheckedException {
        // Create a fake parent AST element for the query to allow replacing the query in the parent by split.
        GridSqlSubquery fakeQryParent = new GridSqlSubquery(qry);

        // Fake parent query model. We need it just for convenience because buildQueryModel needs parent model.
        SplitterQueryModel fakeModelParent = new SplitterQueryModel(null, null, -1, null);

        // Build a simplified query model. We need it because navigation over the original AST is too complex.
        fakeModelParent.buildQueryModel(fakeQryParent, 0, null);

        assert fakeModelParent.childModelsCount() == 1;

        // Get the built query model from the fake parent.
        SplitterQueryModel model = fakeModelParent.childModel(0);

        // Setup the needed information for split.
        model.analyzeQueryModel(collocatedGrpBy);

        // If we have child queries to split, then go hard way.
        if (model.needSplitChild()) {
            // All the siblings to selects we are going to split must be also wrapped into subqueries.
            pushDownQueryModel(model);

            // Need to make all the joined subqueries to be ordered by join conditions.
            setupMergeJoinSorting(model);
        }
        else if (!model.needSplit())  // Just split the top level query.
            model.forceSplit();

        // Split the query model into multiple map queries and a single reduce query.
        splitQueryModel(model);

        // Get back the updated query from the fake parent. It will be our reduce query.
        qry = fakeQryParent.subquery();

        String rdcQry = qry.getSQL();

        SplitterUtils.checkNoDataTablesInReduceQuery(qry, rdcQry);

        // Setup a resulting reduce query.
        rdcSqlQry = new GridCacheSqlQuery(rdcQry);

        skipMergeTbl = qry.skipMergeTable();

        setupParameters(rdcSqlQry, qry, paramsCnt);
    }

    /**
     * @param model Query model.
     */
    private void pushDownQueryModel(SplitterQueryModel model) {
        if (model.type() == SplitterQueryModelType.UNION) {
            assert model.needSplitChild(); // Otherwise we should not get here.

            for (int i = 0; i < model.childModelsCount(); i++)
                pushDownQueryModel(model.childModel(i));
        }
        else if (model.type() == SplitterQueryModelType.SELECT) {
            // If we already need to split, then no need to push down here.
            if (!model.needSplit()) {
                assert model.needSplitChild(); // Otherwise we should not get here.

                pushDownQueryModelSelect(model);
            }
        }
        else
            throw new IllegalStateException("Type: " + model.type());
    }

    /**
     * @param expr Expression.
     * @return {@code true} If the expression contains pushed down columns.
     */
    private boolean hasPushedDownColumn(GridSqlAst expr) {
        if (expr instanceof GridSqlColumn)
            return pushedDownCols.contains(((GridSqlColumn)expr).columnName());

        for (int i = 0; i < expr.size(); i++) {
            if (hasPushedDownColumn(expr.child(i)))
                return true;
        }

        return false;
    }

    /**
     * @param model Query model for the SELECT.
     */
    private void pushDownQueryModelSelect(SplitterQueryModel model) {
        assert model.type() == SplitterQueryModelType.SELECT : model.type();

        boolean hasLeftJoin = SplitterUtils.hasLeftJoin(model.<GridSqlSelect>ast().from());

        int begin = -1;

        // Here we iterate over joined FROM table filters.
        // !!! model.size() can change, never assign it to a variable.
        for (int i = 0; i < model.childModelsCount(); i++) {
            SplitterQueryModel child = model.childModel(i);

            boolean hasPushedDownCol = false;

            // It is either splittable subquery (it must remain in REDUCE query)
            // or left join condition with pushed down columns (this condition
            // can not be pushed down into a wrap query).
            if ((child.isQuery() && (child.needSplitChild() || child.needSplit())) ||
                // We always must be at the right side of the join here to push down
                // range on the left side. If there are no LEFT JOINs in the SELECT, then
                // we will never have ON conditions, they are getting moved to WHERE clause.
                (hasPushedDownCol = (hasLeftJoin && i != 0 && hasPushedDownColumn(model.findJoin(i).on())))) {
                // Handle a single table push down case.
                if (hasPushedDownCol && begin == -1)
                    begin = i - 1;

                // Push down the currently collected range.
                if (begin != -1) {
                    pushDownQueryModelRange(model, begin, i - 1);

                    i = begin + 1; // We've modified model by this range push down, need to adjust counter.

                    assert model.childModel(i) == child; // Adjustment check: we have to return to the same point.

                    // Reset range begin: in case of pushed down column we can assume current child as
                    // as new begin (because it is not a splittable subquery), otherwise reset begin
                    // and try to find next range begin further.
                    begin = hasPushedDownCol ? i : -1;
                }

                if (child.needSplitChild())
                    pushDownQueryModel(child);
            }
            // It is a table or a function or a subquery that we do not need to split or split child.
            // We need to find all the joined elements like this in chain to push them down.
            else if (begin == -1)
                begin = i;
        }

        // Push down the remaining range.
        if (begin != -1)
            pushDownQueryModelRange(model, begin, model.childModelsCount() - 1);
    }

    /**
     * @param model Query model.
     */
    private void setupMergeJoinSorting(SplitterQueryModel model) {
        if (model.type() == SplitterQueryModelType.UNION) {
            for (int i = 0; i < model.childModelsCount(); i++)
                setupMergeJoinSorting(model.childModel(i));
        }
        else if (model.type() == SplitterQueryModelType.SELECT) {
            if (!model.needSplit()) {
                boolean needSplitChild = false;

                for (int i = 0; i < model.childModelsCount(); i++) {
                    SplitterQueryModel child = model.childModel(i);

                    assert child.isQuery() : child.type();

                    if (child.needSplit())
                        needSplitChild = true;
                    else
                        setupMergeJoinSorting(child); // Go deeper.
                }

                if (needSplitChild && model.childModelsCount() > 1)
                    setupMergeJoinSortingSelect(model); // Setup merge join hierarchy for the SELECT.
            }
        }
        else
            throw new IllegalStateException("Type: " + model.type());
    }

    /**
     * @param model Query model.
     */
    private void setupMergeJoinSortingSelect(SplitterQueryModel model) {
        // After pushing down we can have the following variants:
        //  - splittable SELECT
        //  - subquery with splittable child (including UNION)

        // We will push down chains of sequential subqueries between
        // the splittable SELECT like in "push down" and
        // setup sorting for all of them by joined fields.

        for (int i = 1; i < model.childModelsCount(); i++) {
            SplitterQueryModel child = model.childModel(i);

            if (child.needSplit()) {
                if (i > 1) {
                    // If we have multiple joined non-splittable subqueries before, then
                    // push down them into a single subquery.
                    doPushDownQueryModelRange(model, 0, i - 1, false);

                    i = 1;

                    assert model.childModel(i) == child; // We must remain at the same position.
                }

                injectSortingFirstJoin(model);
            }
        }
    }

    /**
     * @param model Query model.
     */
    private void injectSortingFirstJoin(SplitterQueryModel model) {
        GridSqlJoin join = model.findJoin(0);

        // We are at the beginning, thus left and right AST must be children of the same join AST.
        //     join2
        //      / \
        //   join1 \
        //    / \   \
        //  T0   T1  T2
        GridSqlAlias leftTbl = (GridSqlAlias)join.leftTable();
        GridSqlAlias rightTbl = (GridSqlAlias)join.rightTable();

        // Collect all AND conditions.
        List<SplitterAndCondition> andConditions = new ArrayList<>();

        SplitterAndCondition.collectAndConditions(andConditions, join, ON_CHILD);
        SplitterAndCondition.collectAndConditions(andConditions, model.ast(), WHERE_CHILD);

        // Collect all the JOIN condition columns for sorting.
        List<GridSqlColumn> leftOrder = new ArrayList<>();
        List<GridSqlColumn> rightOrder = new ArrayList<>();

        for (int i = 0; i < andConditions.size(); i++) {
            SplitterAndCondition and = andConditions.get(i);
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
     * @param model Query model.
     * @param begin The first child model in range to push down.
     * @param end The last child model in range to push down.
     */
    private void pushDownQueryModelRange(SplitterQueryModel model, int begin, int end) {
        assert end >= begin;

        if (begin == end && model.childModel(end).isQuery()) {
            // Simple case when we have a single subquery to push down, just mark it to be splittable.
            model.childModel(end).forceSplit();
        }
        else {
            // Here we have to generate a subquery for all the joined elements and
            // and mark that subquery as splittable.
            doPushDownQueryModelRange(model, begin, end, true);
        }
    }

    /**
     * @param model Query model.
     * @param begin The first child model in range to push down.
     * @param end The last child model in range to push down.
     * @param needSplit If we will need to split the created subquery model.
     */
    private void doPushDownQueryModelRange(SplitterQueryModel model, int begin, int end, boolean needSplit) {
        // Create wrap query where we will push all the needed joined elements, columns and conditions.
        GridSqlSelect wrapSelect = new GridSqlSelect();
        GridSqlSubquery wrapSubqry = new GridSqlSubquery(wrapSelect);
        GridSqlAlias wrapAlias = SplitterUtils.alias(nextUniqueTableAlias(null), wrapSubqry);

        SplitterQueryModel wrapModel = new SplitterQueryModel(
            SplitterQueryModelType.SELECT,
            wrapSubqry,
            0,
            wrapAlias,
            needSplit
        );

        // Prepare all the prerequisites.
        GridSqlSelect select = model.ast();

        Set<GridSqlAlias> tblAliases = U.newIdentityHashSet();
        Map<String,GridSqlAlias> cols = new HashMap<>();

        // Collect all the tables for push down.
        for (int i = begin; i <= end; i++) {
            GridSqlAlias uniqueTblAlias = model.childModel(i).uniqueAlias();

            assert uniqueTblAlias != null : select.getSQL();

            tblAliases.add(uniqueTblAlias);
        }

        // Push down columns in SELECT clause.
        pushDownSelectColumns(tblAliases, cols, wrapAlias, select);

        // Move all the related WHERE conditions to wrap query.
        pushDownWhereConditions(tblAliases, cols, wrapAlias, select);

        // Push down to a subquery all the JOIN elements and process ON conditions.
        pushDownJoins(tblAliases, cols, model, begin, end, wrapAlias);

        // Add all the collected columns to the wrap query.
        for (GridSqlAlias col : cols.values())
            wrapSelect.addColumn(col, true);

        // Adjust query models to a new AST structure.

        // Move pushed down child models to the newly created model.
        model.moveChildModelsToWrapModel(wrapModel, begin, end);
    }

    /**
     * @param tblAliases Table aliases for push down.
     * @param cols Columns with generated aliases.
     * @param model Query model.
     * @param begin The first child model in range to push down.
     * @param end The last child model in range to push down.
     * @param wrapAlias Alias of the wrap query.
     */
    private void pushDownJoins(
        Set<GridSqlAlias> tblAliases,
        Map<String,GridSqlAlias> cols,
        SplitterQueryModel model,
        int begin,
        int end,
        GridSqlAlias wrapAlias
    ) {
        // Get the original SELECT.
        GridSqlSelect select = model.ast();

        GridSqlSelect wrapSelect = GridSqlAlias.<GridSqlSubquery>unwrap(wrapAlias).subquery();

        final int last = model.childModelsCount() - 1;

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

            GridSqlJoin endJoin = model.findJoin(end);

            wrapSelect.from(model.childModel(end).uniqueAlias());
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

            GridSqlJoin beginJoin = model.findJoin(begin);
            GridSqlJoin afterBeginJoin = model.findJoin(begin + 1);
            GridSqlJoin endJoin = model.findJoin(end);

            wrapSelect.from(endJoin);
            afterBeginJoin.leftTable(beginJoin.rightTable());
            beginJoin.rightTable(wrapAlias);
            select.from(beginJoin);
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

            GridSqlJoin endJoin = model.findJoin(end);
            GridSqlJoin afterEndJoin = model.findJoin(end + 1);

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

            GridSqlJoin beginJoin = model.findJoin(begin);
            GridSqlJoin afterBeginJoin = model.findJoin(begin + 1);
            GridSqlJoin endJoin = model.findJoin(end);
            GridSqlJoin afterEndJoin = model.findJoin(end + 1);

            wrapSelect.from(endJoin);
            afterEndJoin.leftTable(beginJoin);
            afterBeginJoin.leftTable(beginJoin.rightTable());
            beginJoin.rightTable(wrapAlias);
        }

        GridSqlAst from = select.from();

        // Push down related ON conditions for all the related joins.
        while (from instanceof GridSqlJoin) {
            assert !(((GridSqlJoin)from).rightTable() instanceof GridSqlJoin);

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
    @SuppressWarnings("IfMayBeConditional")
    private void pushDownSelectColumns(
        Set<GridSqlAlias> tblAliases,
        Map<String,GridSqlAlias> cols,
        GridSqlAlias wrapAlias,
        GridSqlSelect select
    ) {
        for (int i = 0; i < select.allColumns(); i++) {
            GridSqlAst expr = select.column(i);

            if (!(expr instanceof GridSqlAlias)) {
                // If this is an expression without alias, we need replace it with an alias,
                // because in the wrapQuery we will generate unique aliases for all the columns to avoid duplicates
                // and all the columns in the will be replaced.
                String alias;

                if (expr instanceof GridSqlColumn)
                    alias = ((GridSqlColumn)expr).columnName();
                else
                    alias = EXPR_ALIAS_PREFIX + i;

                expr = SplitterUtils.alias(alias, expr);

                select.setColumn(i, expr);
            }

            if (isAllRelatedToTables(tblAliases, U.newIdentityHashSet(), expr)
                && !SplitterUtils.hasAggregates(expr)) {
                // Push down the whole expression.
                pushDownColumn(tblAliases, cols, wrapAlias, expr, 0);
            }
            else {
                // Push down each column separately.
                pushDownColumnsInExpression(tblAliases, cols, wrapAlias, expr, 0);
            }
        }
    }

    /**
     * @param tblAliases Table aliases to push down.
     * @param cols Columns with generated aliases.
     * @param wrapAlias Alias of the wrap query.
     * @param parent Parent expression.
     * @param childIdx Child index.
     */
    private void pushDownColumnsInExpression(
        Set<GridSqlAlias> tblAliases,
        Map<String,GridSqlAlias> cols,
        GridSqlAlias wrapAlias,
        GridSqlAst parent,
        int childIdx
    ) {
        GridSqlAst child = parent.child(childIdx);

        if (child instanceof GridSqlColumn)
            pushDownColumn(tblAliases, cols, wrapAlias, parent, childIdx);
        else {
            for (int i = 0; i < child.size(); i++)
                pushDownColumnsInExpression(tblAliases, cols, wrapAlias, child, i);
        }
    }

    /**
     * @param tblAliases Table aliases for push down.
     * @param cols Columns with generated aliases.
     * @param wrapAlias Alias of the wrap query.
     * @param parent Parent element.
     * @param childIdx Child index.
     */
    private void pushDownColumn(
        Set<GridSqlAlias> tblAliases,
        Map<String,GridSqlAlias> cols,
        GridSqlAlias wrapAlias,
        GridSqlAst parent,
        int childIdx
    ) {
        GridSqlAst expr = parent.child(childIdx);

        String uniqueColAlias;

        if (expr instanceof GridSqlColumn) {
            GridSqlColumn col = parent.child(childIdx);

            // It must always be unique table alias.
            GridSqlAlias tblAlias = (GridSqlAlias)col.expressionInFrom();

            assert tblAlias != null; // The query is normalized.

            if (!tblAliases.contains(tblAlias))
                return; // Unrelated column, nothing to do.

            uniqueColAlias = uniquePushDownColumnAlias(col);
        }
        else {
            uniqueColAlias = EXPR_ALIAS_PREFIX + nextExprAliasId++ + "__" + ((GridSqlAlias)parent).alias();
        }

        GridSqlType resType = expr.resultType();
        GridSqlAlias colAlias = cols.get(uniqueColAlias);

        if (colAlias == null) {
            colAlias = SplitterUtils.alias(uniqueColAlias, expr);

            // We have this map to avoid column duplicates in wrap query.
            cols.put(uniqueColAlias, colAlias);

            pushedDownCols.add(uniqueColAlias);
        }

        GridSqlColumn col = SplitterUtils.column(uniqueColAlias);

        col.expressionInFrom(wrapAlias);
        col.resultType(resType);

        parent.child(childIdx, col);
    }

    /**
     * @param col Column.
     * @return Unique column alias based on generated unique table alias.
     */
    private String uniquePushDownColumnAlias(GridSqlColumn col) {
        String colName = col.columnName();

        if (pushedDownCols.contains(colName))
            return colName; // Already pushed down unique alias.

        GridSqlAlias uniqueTblAlias = (GridSqlAlias)col.expressionInFrom();

        return uniquePushDownColumnAlias(uniqueTblAlias.alias(), colName);
    }

    /**
     * @param uniqueTblAlias Unique table alias.
     * @param colName Column name.
     * @return Unique column alias based on generated unique table alias.
     */
    private static String uniquePushDownColumnAlias(String uniqueTblAlias, String colName) {
        assert !F.isEmpty(uniqueTblAlias);
        assert !F.isEmpty(colName);

        return uniqueTblAlias + "__" + colName;
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

        List<SplitterAndCondition> andConditions = new ArrayList<>();

        SplitterAndCondition.collectAndConditions(andConditions, select, WHERE_CHILD);

        for (int i = 0; i < andConditions.size(); i++) {
            SplitterAndCondition c = andConditions.get(i);
            GridSqlAst condition = c.ast();

            if (isAllRelatedToTables(tblAliases, U.newIdentityHashSet(), condition)) {
                if (!SplitterUtils.isTrue(condition)) {
                    // Replace the original condition with `true` and move it to the wrap query.
                    c.parent().child(c.childIndex(), TRUE);

                    wrapSelect.whereAnd(condition);
                }
            }
            else
                pushDownColumnsInExpression(tblAliases, cols, wrapAlias, c.parent(), c.childIndex());
        }
    }

    /**
     * @param tblAliases Table aliases for push down.
     * @param locSubQryTblAliases Local subquery tables.
     * @param ast AST.
     * @return {@code true} If all the columns in the given expression are related to the given tables.
     */
    @SuppressWarnings({"SuspiciousMethodCalls", "RedundantIfStatement"})
    private static boolean isAllRelatedToTables(Set<GridSqlAlias> tblAliases, Set<GridSqlAlias> locSubQryTblAliases,
        GridSqlAst ast) {
        if (ast instanceof GridSqlColumn) {
            GridSqlColumn col = (GridSqlColumn)ast;

            // The column must be related to the given list of tables.
            if (!tblAliases.contains(col.expressionInFrom()) &&
                // Or must be rated to some local expression subquery.
                !locSubQryTblAliases.contains(col.expressionInFrom()))
                return false;
        }
        else {
            // If it is a subquery, collect all the table aliases to ignore them later.
            if (ast instanceof GridSqlSelect)
                ((GridSqlSelect)ast).collectFromAliases(locSubQryTblAliases);

            for (int i = 0; i < ast.size(); i++) {
                if (!isAllRelatedToTables(tblAliases, locSubQryTblAliases, ast.child(i)))
                    return false;
            }
        }

        return true;
    }

    /**
     * @param model Query model.
     */
    private void splitQueryModel(SplitterQueryModel model) throws IgniteCheckedException {
        switch (model.type()) {
            case SELECT:
                if (model.needSplit()) {
                    splitSelect(model.parent(), model.childIndex());

                    break;
                }

                // Intentional fallthrough to go deeper.
            case UNION:
                for (int i = 0; i < model.childModelsCount(); i++)
                    splitQueryModel(model.childModel(i));

                break;

            default:
                throw new IllegalStateException("Type: " + model.type());
        }
    }

    /**
     * !!! Notice that here we will modify the original query AST in this method.
     *
     * @param parent Parent AST element.
     * @param childIdx Index of child select.
     */
    private void splitSelect(GridSqlAst parent, int childIdx) throws IgniteCheckedException {
        if (++splitId > 99)
            throw new CacheException("Too complex query to process.");

        final GridSqlSelect mapQry = parent.child(childIdx);

        final int visibleCols = mapQry.visibleColumns();

        List<GridSqlAst> rdcExps = new ArrayList<>(visibleCols);
        List<GridSqlAst> mapExps = new ArrayList<>(mapQry.allColumns());

        mapExps.addAll(mapQry.columns(false));

        Set<String> colNames = new HashSet<>();
        final int havingCol = mapQry.havingColumn();

        boolean distinctAggregateFound = false;

        if (!collocatedGrpBy) {
            for (int i = 0, len = mapExps.size(); i < len; i++)
                distinctAggregateFound |= SplitterUtils.hasDistinctAggregates(mapExps.get(i));
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
            rdcQry.addColumn(SplitterUtils.column(((GridSqlAlias)mapExps.get(i)).alias()), false);

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
                mapQry.limit(SplitterUtils.op(GridSqlOperationType.PLUS, mapQry.offset(), mapQry.limit()));

            mapQry.offset(null);
        }

        // -- DISTINCT
        if (mapQry.distinct()) {
            mapQry.distinct(!aggregateFound && mapQry.groupColumns() == null && mapQry.havingColumn() < 0);
            rdcQry.distinct(true);
        }

        // Replace the given select with generated reduce query in the parent.
        parent.child(childIdx, rdcQry);

        // Setup resulting map query.
        GridCacheSqlQuery map = new GridCacheSqlQuery(mapQry.getSQL());

        setupParameters(map, mapQry, paramsCnt);

        map.columns(collectColumns(mapExps));
        map.sortColumns(mapQry.sort());
        map.partitioned(SplitterUtils.hasPartitionedTables(mapQry));
        map.hasSubQueries(SplitterUtils.hasSubQueries(mapQry));

        if (map.isPartitioned() && canExtractPartitions)
            map.derivedPartitions(extractor.extract(mapQry));

        mapSqlQrys.add(map);
    }

    /**
     * Retrieves _KEY column from SELECT. This column is used for SELECT FOR UPDATE statements.
     *
     * @param sel Select statement.
     * @return Key column alias.
     */
    public static GridSqlAlias keyColumn(GridSqlSelect sel) {
        GridSqlAst from = sel.from();

        GridSqlTable tbl = from instanceof GridSqlTable ? (GridSqlTable)from :
            ((GridSqlElement)from).child();

        GridH2Table gridTbl = tbl.dataTable();

        Column h2KeyCol = gridTbl.getColumn(QueryUtils.KEY_COL);

        GridSqlColumn keyCol = new GridSqlColumn(h2KeyCol, tbl, h2KeyCol.getName());
        keyCol.resultType(GridSqlType.fromColumn(h2KeyCol));

        GridSqlAlias al = SplitterUtils.alias(QueryUtils.KEY_FIELD_NAME, keyCol);

        return al;
    }

    /**
     * @param sqlQry Query.
     * @param qryAst Select AST.
     * @param paramsCnt Number of parameters.
     */
    private static void setupParameters(GridCacheSqlQuery sqlQry, GridSqlQuery qryAst, int paramsCnt) {
        TreeSet<Integer> paramIdxs = new TreeSet<>();

        SplitterUtils.findParamsQuery(qryAst, paramsCnt, paramIdxs);

        int[] paramIdxsArr = new int[paramIdxs.size()];

        int i = 0;

        for (Integer paramIdx : paramIdxs)
            paramIdxsArr[i++] = paramIdx;

        sqlQry.parameterIndexes(paramIdxsArr);
    }

    /**
     * @param cols Columns from SELECT clause.
     * @return Map of columns with types.
     */
    @SuppressWarnings("IfMayBeConditional")
    private LinkedHashMap<String,?> collectColumns(List<GridSqlAst> cols) {
        LinkedHashMap<String, GridSqlType> res = new LinkedHashMap<>(cols.size(), 1f, false);

        for (int i = 0; i < cols.size(); i++) {
            GridSqlAst col = cols.get(i);

            GridSqlAst colUnwrapped;
            String alias;

            if (col instanceof GridSqlAlias) {
                colUnwrapped = col.child();

                alias = ((GridSqlAlias)col).alias();
            }
            else {
                colUnwrapped = col;

                alias = columnName(i);
            }

            GridSqlType type = colUnwrapped.resultType();

            if (type == null)
                throw new NullPointerException("Column type: " + col);

            if (res.put(alias, type) != null)
                throw new IllegalStateException("Alias already exists: " + alias);
        }

        return res;
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
     * @param parent Table parent element.
     * @param childIdx Child index for the table or alias containing the table.
     */
    private void generateUniqueAlias(GridSqlAst parent, int childIdx) {
        GridSqlAst child = parent.child(childIdx);
        GridSqlAst tbl = GridSqlAlias.unwrap(child);

        assert tbl instanceof GridSqlTable || tbl instanceof GridSqlSubquery ||
            tbl instanceof GridSqlFunction : tbl.getClass();

        String uniqueAlias = nextUniqueTableAlias(tbl != child ? ((GridSqlAlias)child).alias() : null);

        GridSqlAlias uniqueAliasAst = new GridSqlAlias(uniqueAlias, tbl);

        uniqueFromAliases.put(tbl, uniqueAliasAst);

        // Replace the child in the parent.
        parent.child(childIdx, uniqueAliasAst);
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
     * @param parent Parent element.
     * @param childIdx Child index.
     * @param parentAlias If the parent is {@link GridSqlAlias}.
     */
    private void normalizeFrom(GridSqlAst parent, int childIdx, boolean parentAlias) {
        GridSqlElement from = parent.child(childIdx);

        if (from instanceof GridSqlTable) {
            GridSqlTable tbl = (GridSqlTable)from;

            String schemaName = tbl.dataTable() != null ? tbl.dataTable().identifier().schema() : tbl.schema();
            String tblName = tbl.dataTable() != null ? tbl.dataTable().identifier().table() : tbl.tableName();

            tbls.add(new QueryTable(schemaName, tblName));

            // In case of alias parent we need to replace the alias itself.
            if (!parentAlias)
                generateUniqueAlias(parent, childIdx);
        }
        else if (from instanceof GridSqlAlias) {
            // Replace current alias with generated unique alias.
            normalizeFrom(from, 0, true);
            generateUniqueAlias(parent, childIdx);
        }
        else if (from instanceof GridSqlSubquery) {
            // We do not need to wrap simple functional subqueries into filtering function,
            // because we can not have any other tables than Ignite (which are already filtered)
            // and functions we have to filter explicitly as well.
            normalizeQuery(((GridSqlSubquery)from).subquery());

            if (!parentAlias) // H2 generates aliases for subqueries in FROM clause.
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
            // In case of alias parent we need to replace the alias itself.
            if (!parentAlias)
                generateUniqueAlias(parent, childIdx);
        }
        else
            throw new IllegalStateException(from.getClass().getName() + " : " + from.getSQL());
    }

    /**
     * @param parent Parent element.
     * @param childIdx Child index.
     */
    @SuppressWarnings("StatementWithEmptyBody")
    private void normalizeExpression(GridSqlAst parent, int childIdx) {
        GridSqlAst el = parent.child(childIdx);

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
            assert uniqueAlias != null : childIdx + "\n" + parent.getSQL();

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

        if (!collocatedGrpBy && SplitterUtils.hasAggregates(el)) {
            aggregateFound = true;

            if (alias == null)
                alias = SplitterUtils.alias(isHaving ? HAVING_COLUMN : columnName(idx), el);

            // We can update original alias here as well since it will be dropped from mapSelect.
            splitAggregates(alias, 0, mapSelect, idx, hasDistinctAggregate, true);

            rdcSelect.add(alias);
        }
        else {
            String mapColAlias = isHaving ? HAVING_COLUMN : columnName(idx);
            String rdcColAlias;

            if (alias == null)  // Original column name for reduce column.
                rdcColAlias = el instanceof GridSqlColumn ? ((GridSqlColumn)el).columnName() : mapColAlias;
            else // Set initial alias for reduce column.
                rdcColAlias = alias.alias();

            // Always wrap map column into generated alias.
            mapSelect.set(idx, SplitterUtils.alias(mapColAlias, el)); // `el` is known not to be an alias.

            // SELECT __C0 AS original_alias
            GridSqlElement rdcEl = SplitterUtils.column(mapColAlias);

            if (colNames.add(rdcColAlias)) // To handle column name duplication (usually wildcard for few tables).
                rdcEl = SplitterUtils.alias(rdcColAlias, rdcEl);

            rdcSelect.add(rdcEl);
        }

        return aggregateFound;
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
    @SuppressWarnings("IfMayBeConditional")
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
        GridSqlAlias mapAggAlias = SplitterUtils.alias(columnName(first ? exprIdx : mapSelect.size()), EMPTY);

        // Replace original expression if it is the first aggregate in expression or add to the end.
        if (first)
            mapSelect.set(exprIdx, mapAggAlias);
        else
            mapSelect.add(mapAggAlias);

        /* Note Distinct aggregate can be performed only on reduce phase, so
           if query contains distinct aggregate then other aggregates must be processed the same way. */
        switch (agg.type()) {
            case AVG: // SUM( AVG(CAST(x AS DOUBLE))*COUNT(x) )/SUM( COUNT(x) )  or  AVG(x)
                if (hasDistinctAggregate) /* and has no collocated group by */ {
                    mapAgg = agg.child();

                    rdcAgg = SplitterUtils.aggregate(agg.distinct(), agg.type()).resultType(agg.resultType())
                        .addChild(SplitterUtils.column(mapAggAlias.alias()));
                }
                else {
                    //-- COUNT(x) map
                    GridSqlElement cntMapAgg = SplitterUtils.aggregate(agg.distinct(), COUNT)
                        .resultType(GridSqlType.BIGINT).addChild(agg.child());

                    // Add generated alias to COUNT(x).
                    // Using size as index since COUNT will be added as the last select element to the map query.
                    String cntMapAggAlias = columnName(mapSelect.size());

                    cntMapAgg = SplitterUtils.alias(cntMapAggAlias, cntMapAgg);

                    mapSelect.add(cntMapAgg);

                    //-- AVG(CAST(x AS DOUBLE)) map
                    mapAgg = SplitterUtils.aggregate(agg.distinct(), AVG).resultType(GridSqlType.DOUBLE).addChild(
                        new GridSqlFunction(CAST).resultType(GridSqlType.DOUBLE).addChild(agg.child()));

                    //-- SUM( AVG(x)*COUNT(x) )/SUM( COUNT(x) ) reduce
                    GridSqlElement sumUpRdc = SplitterUtils.aggregate(false, SUM).addChild(
                        SplitterUtils.op(GridSqlOperationType.MULTIPLY,
                            SplitterUtils.column(mapAggAlias.alias()),
                            SplitterUtils.column(cntMapAggAlias)));

                    GridSqlElement sumDownRdc =
                        SplitterUtils.aggregate(false, SUM).addChild(SplitterUtils.column(cntMapAggAlias));

                    if (!SplitterUtils.isFractionalType(agg.resultType().type())) {
                        sumUpRdc = new GridSqlFunction(CAST).resultType(GridSqlType.BIGINT).addChild(sumUpRdc);
                        sumDownRdc = new GridSqlFunction(CAST).resultType(GridSqlType.BIGINT).addChild(sumDownRdc);
                    }

                    rdcAgg = new GridSqlFunction(CAST).resultType(agg.resultType())
                        .addChild(SplitterUtils.op(GridSqlOperationType.DIVIDE, sumUpRdc, sumDownRdc));
                }

                break;

            case SUM: // SUM( SUM(x) ) or SUM(DISTINCT x)
            case MAX: // MAX( MAX(x) ) or MAX(DISTINCT x)
            case MIN: // MIN( MIN(x) ) or MIN(DISTINCT x)
                GridSqlElement rdcAgg0;

                if (hasDistinctAggregate) /* and has no collocated group by */ {
                    mapAgg = agg.child();

                    rdcAgg0 = SplitterUtils.aggregate(agg.distinct(), agg.type())
                        .addChild(SplitterUtils.column(mapAggAlias.alias()));
                }
                else {
                    mapAgg = SplitterUtils.aggregate(agg.distinct(), agg.type()).resultType(agg.resultType())
                        .addChild(agg.child());

                    rdcAgg0 = new GridSqlFunction(CAST).resultType(agg.resultType())
                        .addChild(SplitterUtils.aggregate(agg.distinct(), agg.type())
                            .addChild(SplitterUtils.column(mapAggAlias.alias()))
                        );
                }

                // Avoid second type upcast on reducer (e.g. Int -> (map) -> Long -> (reduce) -> BigDecimal).
                rdcAgg = new GridSqlFunction(CAST).resultType(agg.resultType()).addChild(rdcAgg0);

                break;

            case COUNT_ALL: // CAST(SUM( COUNT(*) ) AS BIGINT)
            case COUNT: // CAST(SUM( COUNT(x) ) AS BIGINT) or CAST(COUNT(DISTINCT x) AS BIGINT)
                if (hasDistinctAggregate) /* and has no collocated group by */ {
                    assert agg.type() == COUNT;

                    mapAgg = agg.child();

                    rdcAgg = SplitterUtils.aggregate(agg.distinct(), agg.type()).resultType(GridSqlType.BIGINT)
                        .addChild(SplitterUtils.column(mapAggAlias.alias()));
                }
                else {
                    mapAgg = SplitterUtils.aggregate(agg.distinct(), agg.type()).resultType(GridSqlType.BIGINT);

                    if (agg.type() == COUNT)
                        mapAgg.addChild(agg.child());

                    rdcAgg = SplitterUtils.aggregate(false, SUM).addChild(SplitterUtils.column(mapAggAlias.alias()));
                    rdcAgg = new GridSqlFunction(CAST).resultType(GridSqlType.BIGINT).addChild(rdcAgg);
                }

                break;

            case GROUP_CONCAT:
                if (agg.distinct() || agg.hasGroupConcatOrder()) {
                    throw new IgniteSQLException("Clauses DISTINCT and ORDER BY are unsupported for GROUP_CONCAT " +
                        "for not collocated data.", IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
                }

                if (hasDistinctAggregate)
                    mapAgg = agg.child();
                else {
                    mapAgg = SplitterUtils
                        .aggregate(agg.distinct(), agg.type())
                        .setGroupConcatSeparator(agg.getGroupConcatSeparator())
                        .resultType(GridSqlType.STRING)
                        .addChild(agg.child());
                }

                rdcAgg = SplitterUtils.aggregate(false, GROUP_CONCAT)
                    .setGroupConcatSeparator(agg.getGroupConcatSeparator())
                    .resultType(GridSqlType.STRING)
                    .addChild(SplitterUtils.column(mapAggAlias.alias()));

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
     * Get optimized prepared statement.
     *
     * @param c Connection.
     * @param qry Parsed query.
     * @param enforceJoinOrder Enforce join order.
     * @return Optimized prepared command.
     * @throws SQLException If failed.
     */
    private static Prepared prepare(H2PooledConnection c, QueryContext qctx, String qry, boolean distributedJoins,
        boolean enforceJoinOrder) throws SQLException, IgniteCheckedException {
        H2Utils.setupConnection(c, qctx, distributedJoins, enforceJoinOrder);

        try (PreparedStatement s = c.prepareStatement(qry, H2StatementCache.queryFlags(distributedJoins, enforceJoinOrder))) {
            return GridSqlQueryParser.prepared(s);
        }
    }
}
