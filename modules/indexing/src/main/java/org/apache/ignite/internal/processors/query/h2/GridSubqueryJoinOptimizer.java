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

package org.apache.ignite.internal.processors.query.h2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2ProxyIndex;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlAggregateFunction;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlAlias;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlArray;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlAst;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlColumn;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlConst;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlElement;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlFunction;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlJoin;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperation;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQuery;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlSelect;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlSubquery;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlTable;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlUnion;
import org.apache.ignite.internal.util.typedef.F;
import org.h2.index.Index;
import org.h2.table.Column;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperationType.AND;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperationType.EQUAL;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperationType.EQUAL_NULL_SAFE;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperationType.EXISTS;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperationType.IN;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperationType.NEGATE;

/** */
public class GridSubqueryJoinOptimizer {
    /** Predicat returns {@code true} if current element has subquery in its children. */
    private static final BiPredicate<GridSqlAst, GridSqlAst> ELEMENT_WITH_SUBQUERY
        = (parent, child) -> child instanceof GridSqlSubquery;

    /**
     * Predicate returns {@code true} in case there is a child
     * which is an IN-expression and has a subquery in its children.
     */
    private static final BiPredicate<GridSqlAst, GridSqlAst> ELEMENT_WITH_SUBQUERY_WITHIN_IN_EXPRESSION
        = (parent, child) -> child instanceof GridSqlOperation && ((GridSqlOperation)child).operationType() == IN
        && child.child(1) instanceof GridSqlSubquery;

    /**
     * Predicate returns {@code true} in case there is a child
     * which is an EXISTS-expression and has a subquery in its children.
     */
    private static final BiPredicate<GridSqlAst, GridSqlAst> ELEMENT_WITH_SUBQUERY_WITHIN_EXISTS_EXPRESSION
        = (parent, child) -> child instanceof GridSqlOperation && ((GridSqlOperation)child).operationType() == EXISTS
        && child.child() instanceof GridSqlSubquery;

    /** Predicat returns {@code true} if current element is an AND operation. */
    private static final Predicate<GridSqlAst> ELEMENT_IS_AND_OPERATION
        = elem -> elem instanceof GridSqlOperation && ((GridSqlOperation)elem).operationType() == AND;

    /**
     * Predicate returns {@code true} in case there is a child which is an alias for a subquery.
     */
    private static final BiPredicate<GridSqlAst, GridSqlAst> ELEMENT_WITH_ALIAS_WITH_SUBQUERY
        = (parent, child) -> child instanceof GridSqlAlias && child.child() instanceof GridSqlSubquery;

    /** Predicat returns {@code true} if current element is a JOIN. */
    private static final Predicate<GridSqlAst> ELEMENT_IS_JOIN
        = elem -> elem instanceof GridSqlJoin;

    /** Predicat returns {@code true} if current element is an EQUAL or EQUAL_NULL_SAFE operation. */
    private static final Predicate<GridSqlAst> ELEMENT_IS_EQ
        = elem -> elem instanceof GridSqlOperation && (EQUAL == ((GridSqlOperation)elem).operationType()
        || EQUAL_NULL_SAFE == ((GridSqlOperation)elem).operationType());

    /**
     * Whether to apply optimization or not.
     */
    private static volatile Boolean optimizationEnabled;

    /**
     * @return {@code true} if optimization should be applied.
     */
    @SuppressWarnings("NonThreadSafeLazyInitialization")
    private static boolean optimizationEnabled() {
        if (optimizationEnabled == null) { // it's OK if this will be initialized several times in case of races
            optimizationEnabled = Boolean.parseBoolean(
                System.getProperty(IgniteSystemProperties.IGNITE_ENABLE_SUBQUERY_REWRITE_OPTIMIZATION, "true")
            );
        }

        return optimizationEnabled;
    }

    /**
     * Pulls out subquery from parent query where possible.
     *
     * @param parent Parent query where to find and pull out subqueries.
     */
    public static void pullOutSubQueries(GridSqlQuery parent) {
        if (!optimizationEnabled())
            return;

        if (parent instanceof GridSqlUnion) {
            GridSqlUnion union = (GridSqlUnion)parent;

            pullOutSubQueries(union.left());
            pullOutSubQueries(union.right());

            return;
        }

        assert parent instanceof GridSqlSelect : "\"parent\" should be instance of GridSqlSelect class";

        GridSqlSelect select = (GridSqlSelect)parent;

        pullOutSubQryFromSelectExpr(select);
        pullOutSubQryFromInClause(select);
        pullOutSubQryFromExistsClause(select);
        pullOutSubQryFromTableList(select);
    }

    /**
     * Pulls out subquery from select expression.
     *
     * @param select Parent query where to find and pull out subqueries.
     */
    private static void pullOutSubQryFromSelectExpr(GridSqlSelect select) {
        for (int i = 0; i < select.allColumns(); i++) {
            boolean wasPulledOut = false;
            GridSqlAst col = select.columns(false).get(i);

            if (col instanceof GridSqlSubquery)
                wasPulledOut = pullOutSubQryFromSelectExpr(select, null, i);
            else if (col instanceof GridSqlOperation && ((GridSqlOperation)col).operationType() == EXISTS)
                //We actially can't rewrite select query under exists operator. So we skip it.
                continue;
            else {
                ASTNodeFinder finder = new ASTNodeFinder(
                    col,
                    ELEMENT_WITH_SUBQUERY
                );

                ASTNodeFinder.Result res;
                while ((res = finder.findNext()) != null)
                    wasPulledOut |= pullOutSubQryFromSelectExpr(select, res.getEl(), res.getIdx());
            }

            if (wasPulledOut) // we have to analyze just pulled out element as well
                i--;
        }
    }

    /**
     * Pulls out subquery from table list.
     *
     * @param select Parent query where to find and pull out subqueries.
     */
    private static void pullOutSubQryFromTableList(GridSqlSelect select) {
        boolean wasPulledOut;
        do {
            wasPulledOut = false;

            // we have to check the root of the FROM clause in the loop
            // to handle simple hierarchical queries like this:
            // select * from (select * from (select id, name from emp))
            if (ELEMENT_WITH_ALIAS_WITH_SUBQUERY.test(null, select.from()))
                wasPulledOut = pullOutSubQryFromTableList(select, null, -1);
            else if (ELEMENT_IS_JOIN.test(select.from())) {
                ASTNodeFinder finder = new ASTNodeFinder(
                    select.from(),
                    ELEMENT_WITH_ALIAS_WITH_SUBQUERY,
                    ELEMENT_IS_JOIN
                );

                ASTNodeFinder.Result res;
                while ((res = finder.findNext()) != null)
                    wasPulledOut |= pullOutSubQryFromTableList(select, res.getEl(), res.getIdx());
            }
        }
        while (wasPulledOut);
    }

    /**
     * Pulls out subquery from IN expression.
     *
     * @param select Parent query where to find and pull out subqueries.
     */
    private static void pullOutSubQryFromInClause(GridSqlSelect select) {
        // for now it's not possible to have a subquery on top of WHERE clause tree after rewriting,
        // so we could safely process it only once outside the loop
        if (ELEMENT_WITH_SUBQUERY_WITHIN_IN_EXPRESSION.test(null, select.where()))
            pullOutSubQryFromInClause(select, null, -1);

        if (!ELEMENT_IS_AND_OPERATION.test(select.where()))
            return;

        boolean wasPulledOut;
        do {
            wasPulledOut = false;

            ASTNodeFinder finder = new ASTNodeFinder(
                select.where(),
                ELEMENT_WITH_SUBQUERY_WITHIN_IN_EXPRESSION,
                ELEMENT_IS_AND_OPERATION
            );

            ASTNodeFinder.Result res;
            while ((res = finder.findNext()) != null)
                wasPulledOut |= pullOutSubQryFromInClause(select, res.getEl(), res.getIdx());
        }
        while (wasPulledOut);
    }

    /**
     * Pulls out subquery from EXISTS expression.
     *
     * @param select Parent query where to find and pull out subqueries.
     */
    private static void pullOutSubQryFromExistsClause(GridSqlSelect select) {
        // for now it's not possible to have a subquery on top of WHERE clause tree after rewriting,
        // so we could safely process it only once outside the loop
        if (ELEMENT_WITH_SUBQUERY_WITHIN_EXISTS_EXPRESSION.test(null, select.where()))
            pullOutSubQryFromExistsClause(select, null, -1);

        if (!ELEMENT_IS_AND_OPERATION.test(select.where()))
            return;

        boolean wasPulledOut;
        do {
            wasPulledOut = false;

            ASTNodeFinder finder = new ASTNodeFinder(
                select.where(),
                ELEMENT_WITH_SUBQUERY_WITHIN_EXISTS_EXPRESSION,
                ELEMENT_IS_AND_OPERATION
            );

            ASTNodeFinder.Result res;
            while ((res = finder.findNext()) != null)
                wasPulledOut |= pullOutSubQryFromExistsClause(select, res.getEl(), res.getIdx());
        }
        while (wasPulledOut);
    }

    /**
     * Whether Select query is simple or not.
     * <p>
     * We call query simple if it is select query (not union) and it has neither having nor grouping,
     * has no distinct clause, has no aggregations, has no limits, no sorting, no offset clause.
     * Also it is not SELECT FOR UPDATE.
     *
     * @param subQry Sub query.
     * @return {@code true} if it is simple query.
     */
    private static boolean isSimpleSelect(GridSqlQuery subQry) {
        if (subQry instanceof GridSqlUnion)
            return false;

        GridSqlSelect select = (GridSqlSelect)subQry;

        boolean simple = F.isEmpty(select.sort())
            && select.offset() == null
            && select.limit() == null
            && !select.isForUpdate()
            && !select.distinct()
            && select.havingColumn() < 0
            && F.isEmpty(select.groupColumns());

        if (!simple)
            return false;

        for (GridSqlAst col : select.columns(true)) {
            if (!(col instanceof GridSqlElement))
                continue;

            // we have to traverse the tree because there may be such expressions
            // like ((MAX(col) - MIN(col)) / COUNT(col)
            ASTNodeFinder aggFinder = new ASTNodeFinder(
                col,
                (p, c) -> p instanceof GridSqlAggregateFunction
            );

            if (aggFinder.findNext() != null)
                return false;

            // In case of query like "SELECT * FROM (SELECT i||j FROM t) u;", where subquery contains pure operation
            // without an alias, we cannot determine which generated alias in the parent query the original expression
            // belongs to. So the best we can do is skip the case.
            ASTNodeFinder operationFinder = new ASTNodeFinder(
                col,
                (p, c) -> p instanceof GridSqlOperation,
                ast -> false
            );

            if (operationFinder.findNext() != null)
                return false;
        }

        return true;
    }

    /**
     * Check whether table has unique index that can be built with provided column set.
     *
     * @param gridCols Columns.
     * @param tbl Table.
     * @return {@code true} if there is the unique index.
     */
    private static boolean isUniqueIndexExists(Collection<GridSqlColumn> gridCols, GridSqlTable tbl) {
        Set<Column> cols = gridCols.stream().map(GridSqlColumn::column).collect(Collectors.toSet());

        for (Index idx : tbl.dataTable().getIndexes()) {
            // if index is unique
            if ((idx.getIndexType().isUnique()
                // or index is a proxy to the unique index
                || (idx instanceof GridH2ProxyIndex && ((GridH2ProxyIndex)idx).underlyingIndex().getIndexType().isUnique()))
                // and index can be built with provided columns
                && cols.containsAll(Arrays.asList(idx.getColumns())))

                return true;
        }

        return false;
    }

    /**
     * Pull out sub-select from table list.
     * <p>
     * Example:
     * <pre>
     *   Before:
     *     SELECT d.name
     *       FROM emp e,
     *            (select * from dep) d
     *      WHERE e.dep_id = d.id
     *        AND e.sal > 2000
     *
     *   After:
     *     SELECT d.name
     *       FROM emp e
     *       JOIN dep d
     *      WHERE sal > 2000 AND d.id = e.dep_id
     * </pre>
     *
     * @param parent Parent select.
     * @param target Target sql element. Can be {@code null}.
     * @param childInd Column index.
     */
    private static boolean pullOutSubQryFromTableList(
        GridSqlSelect parent,
        @Nullable GridSqlAst target,
        int childInd
    ) {
        if (target != null && !ELEMENT_IS_JOIN.test(target))
            return false;

        GridSqlAlias wrappedSubQry = target != null
            ? target.child(childInd)
            : (GridSqlAlias)parent.from();

        GridSqlSubquery subQry = GridSqlAlias.unwrap(wrappedSubQry);

        if (!isSimpleSelect(subQry.subquery()))
            return false;

        GridSqlSelect subSel = subQry.subquery();

        if (!(subSel.from() instanceof GridSqlAlias) && !(subSel.from() instanceof GridSqlTable))
            return false; // we can't deal with joins and others right now

        GridSqlAlias subTbl = new GridSqlAlias(wrappedSubQry.alias(), GridSqlAlias.unwrap(subSel.from()));
        if (target == null)
            // it's true only when the subquery is only table in the table list
            // so we can safely replace entire FROM expression of the parent
            parent.from(subTbl);
        else
            target.child(childInd, subTbl);

        GridSqlAst where = subSel.where();

        if (where != null) {
            if (target instanceof GridSqlJoin && childInd != GridSqlJoin.LEFT_TABLE_CHILD) {
                GridSqlJoin join = (GridSqlJoin)target;

                join.child(GridSqlJoin.ON_CHILD, new GridSqlOperation(AND, join.on(), where));
            }
            else
                parent.where(parent.where() == null ? where : new GridSqlOperation(AND, parent.where(), where));
        }

        remapColumns(
            parent,
            subSel,
            // reference equality used intentionally here
            col -> wrappedSubQry == col.expressionInFrom(),
            subTbl
        );

        return true;
    }

    /**
     * Remap all columns that satisfy the predicate such they be referred to the given table.
     *
     * @param parent Tree where to search columns.
     * @param subSelect Tree where to search column aliases.
     * @param colPred Collection predicate.
     * @param tbl Table.
     */
    private static void remapColumns(GridSqlAst parent, GridSqlAst subSelect, Predicate<GridSqlColumn> colPred, GridSqlAlias tbl) {
        ASTNodeFinder colFinder = new ASTNodeFinder(
            parent,
            (p, c) -> c instanceof GridSqlColumn && colPred.test((GridSqlColumn)c)
        );

        ASTNodeFinder.Result res;
        while ((res = colFinder.findNext()) != null) {
            GridSqlColumn oldCol = res.getEl().child(res.getIdx());

            BiPredicate<GridSqlAst, GridSqlAst> constPred = (p, c) ->
                c != null && c.getSQL().equals(oldCol.columnName());

            BiPredicate<GridSqlAst, GridSqlAst> aliasPred = (p, c) ->
                c instanceof GridSqlAlias && ((GridSqlAlias)c).alias().equals(oldCol.columnName());

            ASTNodeFinder.Result aliasOrPred = findNode(subSelect, constPred.or(aliasPred));

            if (aliasOrPred != null)
                res.getEl().child(res.getIdx(), GridSqlAlias.unwrap(aliasOrPred.getEl().child(aliasOrPred.getIdx())));
            else {
                res.getEl().child(
                    res.getIdx(),
                    new GridSqlColumn(
                        oldCol.column(),
                        tbl,
                        oldCol.schema(),
                        tbl.alias(),
                        oldCol.columnName()
                    )
                );
            }
        }
    }

    /**
     * Searches for first node in AST tree according to the given parameters.
     *
     * @param tree Parent ast.
     * @param pred Filter predicate.
     * @return Found node or null.
     */
    private static ASTNodeFinder.Result findNode(GridSqlAst tree, BiPredicate<GridSqlAst, GridSqlAst> pred) {
        ASTNodeFinder colFinder = new ASTNodeFinder(tree, pred);

        return colFinder.findNext();
    }

    /**
     * Pull out sub-select from SELECT clause to the parent select level.
     * <p>
     * Example:
     * <pre>
     *   Before:
     *     SELECT name,
     *            (SELECT name FROM dep d WHERE d.id = e.dep_id) as dep_name
     *       FROM emp e
     *      WHERE sal > 2000
     *
     *   After:
     *     SELECT name,
     *            d.name as dep_name
     *       FROM emp e
     *       LEFT JOIN dep d
     *         ON d.id = e.dep_id
     *      WHERE sal > 2000
     * </pre>
     *
     * @param parent Parent select.
     * @param targetEl Target sql element. Can be {@code null}.
     * @param childInd Column ind.
     */
    private static boolean pullOutSubQryFromSelectExpr(
        GridSqlSelect parent,
        @Nullable GridSqlAst targetEl,
        int childInd
    ) {
        GridSqlSubquery subQry = targetEl != null
            ? targetEl.child(childInd)
            : (GridSqlSubquery)parent.columns(false).get(childInd);

        if (!subQueryCanBePulledOut(subQry))
            return false;

        GridSqlSelect subS = subQry.subquery();

        if (subS.allColumns() != 1)
            return false;

        GridSqlAst subCol = GridSqlAlias.unwrap(subS.columns(false).get(0));
        
        // If a constant is selected in a subquery, we cannot put it in parent query without
        // consequences for the correctness of the result.
        // For example, select (select 1 from x where id = 1) becomes select 1 left join x...,
        // and the where condition becomes meaningless.
        
        if (subCol instanceof GridSqlConst)
            return false;
        if (targetEl != null)
            targetEl.child(childInd, subCol);
        else
            parent.setColumn(childInd, subCol);

        GridSqlElement parentFrom = parent.from() instanceof GridSqlElement
            ? (GridSqlElement)parent.from()
            : new GridSqlSubquery((GridSqlQuery)parent.from());

        parent.from(new GridSqlJoin(parentFrom, GridSqlAlias.unwrap(subS.from()),
            true, (GridSqlElement)subS.where())).child();

        return true;
    }

    /**
     * Pull out sub-select from IN clause to the parent select level.
     * <p>
     * Example:
     * <pre>
     *   Before:
     *     SELECT name
     *       FROM emp e
     *      WHERE e.dep_id IN (SELECT d.id FROM dep d WHERE d.name = 'dep1')
     *        AND sal > 2000
     *
     *   After:
     *     SELECT name
     *       FROM emp e
     *       JOIN dep d
     *      WHERE sal > 2000 AND d.id = e.dep_id and d.name = 'dep1
     * </pre>
     *
     * @param parent Parent select.
     * @param targetEl Target sql element. Can be {@code null}.
     * @param childInd Column index.
     */
    private static boolean pullOutSubQryFromInClause(
        GridSqlSelect parent,
        @Nullable GridSqlAst targetEl,
        int childInd
    ) {
        // extract sub-query
        GridSqlSubquery subQry = targetEl != null
            ? targetEl.child(childInd).child(1)
            : parent.where().child(1);

        if (!isSimpleSelect(subQry.subquery()))
            return false;

        GridSqlSelect subS = subQry.subquery();

        GridSqlAst leftExpr = targetEl != null
            ? targetEl.child(childInd).child(0)
            : parent.where().child(0);

        // should be allowed only following variation:
        //      1) tbl_col IN (SELECT in_col FROM ...)
        //      2) (c1, c2, ..., cn) IN (SELECT (c1, c2, ..., cn) FROM ...)
        //      3) const IN (SELECT in_col FROM ...)
        //      4) c1 + c2/const IN (SELECT in_col FROM ...)
        if (subS.visibleColumns() != 1)
            return false;

        List<GridSqlElement> conditions = new ArrayList<>();

        if (leftExpr instanceof GridSqlArray) {
            GridSqlAst col = subS.columns(true).get(0);

            if (!(col instanceof GridSqlArray) || leftExpr.size() != col.size())
                return false;

            for (int i = 0; i < col.size(); i++) {
                GridSqlElement el = leftExpr.child(i);

                conditions.add(new GridSqlOperation(EQUAL, el, col.child(i)));
            }
        }
        else if (targetEl instanceof GridSqlFunction)
            return false;
        else
            conditions.add(new GridSqlOperation(EQUAL, leftExpr, subS.columns(true).get(0)));

        // save old condition and IN expression to restore them
        // in case of unsuccessfull pull out
        GridSqlElement oldCond = (GridSqlElement)subS.where();
        if (oldCond != null)
            conditions.add(oldCond);

        GridSqlElement oldInExpr = targetEl != null
            ? targetEl.child(childInd)
            : (GridSqlElement)parent.where();

        // update sub-query condition and convert IN clause to EXISTS
        subS.where(buildConditionBush(conditions));

        GridSqlOperation existsExpr = new GridSqlOperation(EXISTS, subQry);
        if (targetEl != null)
            targetEl.child(childInd, existsExpr);
        else
            parent.where(existsExpr);

        boolean res = pullOutSubQryFromExistsClause(parent, targetEl, childInd);

        if (!res) { // restore original query in case of failure
            if (targetEl != null)
                targetEl.child(childInd, oldInExpr);
            else
                parent.where(oldInExpr);

            subS.where(oldCond);
        }

        return res;
    }

    /**
     * Creates tree from provided elements which will be connected with an AND operator.
     *
     * @param ops Ops.
     * @return Root of the resulting tree.
     */
    private static GridSqlElement buildConditionBush(List<GridSqlElement> ops) {
        assert !F.isEmpty(ops);

        if (ops.size() == 1)
            return ops.get(0);

        int m = (ops.size() + 1) / 2;

        GridSqlElement left = buildConditionBush(ops.subList(0, m));
        GridSqlElement right = buildConditionBush(ops.subList(m, ops.size()));

        return new GridSqlOperation(AND, left, right);
    }

    /**
     * Pull out sub-select from EXISTS clause to the parent select level.
     * <p>
     * Example:
     * <pre>
     *   Before:
     *     SELECT name
     *       FROM emp e
     *      WHERE EXISTS (SELECT 1 FROM dep d WHERE d.id = e.dep_id and d.name = 'dep1')
     *        AND sal > 2000
     *
     *   After:
     *     SELECT name
     *       FROM emp e
     *       JOIN dep d
     *      WHERE sal > 2000 AND d.id = e.dep_id and d.name = 'dep1
     * </pre>
     *
     * @param parent Parent select.
     * @param targetEl Target sql element. Can be null.
     * @param childInd Column ind.
     */
    private static boolean pullOutSubQryFromExistsClause(
        GridSqlSelect parent,
        @Nullable GridSqlAst targetEl,
        int childInd
    ) {
        // extract sub-query
        GridSqlSubquery subQry = targetEl != null
            ? targetEl.child(childInd).child()
            : parent.where().child();

        if (!subQueryCanBePulledOut(subQry))
            return false;

        GridSqlSelect subS = subQry.subquery();

        if (targetEl != null)
            targetEl.child(childInd, subS.where());
        else
            parent.where(subS.where());

        GridSqlElement parentFrom = parent.from() instanceof GridSqlElement
            ? (GridSqlElement)parent.from()
            : new GridSqlSubquery((GridSqlQuery)parent.from());

        parent.from(new GridSqlJoin(parentFrom, GridSqlAlias.unwrap(subS.from()), false, null)).child();

        return true;
    }

    /**
     * Whether sub-query can be pulled out or not.
     *
     * @param subQry Sub-query.
     * @return {@code true} if sub-query cound be pulled out, {@code false} otherwise.
     */
    private static boolean subQueryCanBePulledOut(GridSqlSubquery subQry) {
        GridSqlQuery subQ = subQry.subquery();

        if (!isSimpleSelect(subQ))
            return false;

        assert subQ instanceof GridSqlSelect;

        GridSqlSelect subS = (GridSqlSelect)subQ;

        // Ensure that sub-query returns at most one row.
        // For now we say, that it is true when there is set of columns such:
        //      1) column from sub-query table
        //      2) can be accessible by any AND branch of the WHERE tree:
        //                  WHERE
        //                    |
        //                   AND
        //                  /    \
        //                AND    OR
        //               /   \   / \
        //              c1   c2 *   * // * - can't be reached by AND branch
        //
        //      2) is strict predicate to sub-query table (only equality supported)
        //      3) this set is superset for any of unique index, e.g. for query like
        //                 WHERE c1 = v1
        //                   AND c2 = v2
        //                         ...
        //                   AND ci = vi
        //                   AND ci+1 = vi+1
        //                   AND ci+2 = vi+2,
        //         there is unique index unique_idx(c1, c2, ..., ci)
        //
        // Nota bene: equality should be only between columns of different tables
        // (or between column and constant\param), because condition like 'PK = PK'
        // filter out nothing.

        if (subS.where() == null)
            return false;

        Predicate<GridSqlAst> andOrEqPre = ELEMENT_IS_AND_OPERATION.or(ELEMENT_IS_EQ);

        if (!andOrEqPre.test(subS.where()))
            return false;

        ASTNodeFinder opEqFinder = new ASTNodeFinder(
            subS.where(),
            (parent, child) -> ELEMENT_IS_EQ.test(parent),
            andOrEqPre
        );

        List<GridSqlColumn> sqlCols = new ArrayList<>();
        ASTNodeFinder.Result res;
        while ((res = opEqFinder.findNext()) != null) {
            GridSqlOperation op = (GridSqlOperation)res.getEl();

            assert op.size() == 2;

            GridSqlColumn[] colArr = new GridSqlColumn[2];

            for (int i = 0; i < 2; i++) {
                if (op.child(i) instanceof GridSqlColumn)
                    colArr[i] = op.child(i);
                else if (op.child(i) instanceof GridSqlOperation
                    && ((GridSqlOperation)op.child(i)).operationType() == NEGATE
                    && op.child() instanceof GridSqlColumn)
                    colArr[i] = op.child(i).child();
            }

            if (colArr[0] == null || colArr[1] == null
                // reference equality used on purpose here, because we should to get known
                // whether the column refers to exactly the same table
                || colArr[0].expressionInFrom() != colArr[1].expressionInFrom()
            ) {
                if (colArr[0] != null && colArr[0].expressionInFrom() == subS.from())
                    sqlCols.add(colArr[0]);

                if (colArr[1] != null && colArr[1].expressionInFrom() == subS.from())
                    sqlCols.add(colArr[1]);
            }
        }

        GridSqlAst subTbl = GridSqlAlias.unwrap(subS.from());

        return subTbl instanceof GridSqlTable && isUniqueIndexExists(sqlCols, (GridSqlTable)subTbl);
    }

    /**
     * Abstract syntax tree node finder.
     * Finds all nodes that satisfies the {@link ASTNodeFinder#emitPred} in depth-first manner.
     */
    private static class ASTNodeFinder {
        /** Iterates over children of tree node. */
        private static class ChildrenIterator {
            /** Index of the current child. */
            private int childInd;

            /** Element on whose children iterate. */
            private final GridSqlAst el;

            /**
             * @param el Element on whose children iterate.
             */
            private ChildrenIterator(GridSqlAst el) {
                this.el = el;

                childInd = -1;
            }

            /**
             * Returns next child.
             * @return Next child or {@code null}, if there are no more children.
             */
            private GridSqlAst nextChild() {
                if (++childInd < el.size())
                    return el.child(childInd);

                return null;
            }
        }

        /** Result of the iteration. */
        private static class Result {
            /** Index of the child which satisfies the emit predicate. */
            private final int idx;

            /** Element on whose children iterate. */
            private final GridSqlAst el;

            /**
             * @param idx Index.
             * @param el El.
             */
            private Result(int idx, GridSqlAst el) {
                this.idx = idx;
                this.el = el;
            }

            /**
             * Returns index of the child.
             * @return index of the child.
             */
            public int getIdx() {
                return idx;
            }

            /**
             * Returns parent element.
             * @return parent element.
             */
            public GridSqlAst getEl() {
                return el;
            }
        }

        /** Stack of the tree nodes. */
        private final List<ChildrenIterator> iterStack = new ArrayList<>();

        /** Emit predicate. */
        private final BiPredicate<GridSqlAst, GridSqlAst> emitPred;

        /** Walk predicate. */
        private final Predicate<GridSqlAst> walkPred;

        /**
         * @param root Root element.
         * @param emitPred Emit predicate. Used to decide whether the current node
         *                 should be returned by {@link ASTNodeFinder#findNext()} or not.
         * @param walkPred Walk predicate. Used to decide should finder step into this
         *                 node or just skip it.
         */
        private ASTNodeFinder(
            GridSqlAst root,
            BiPredicate<GridSqlAst, GridSqlAst> emitPred,
            Predicate<GridSqlAst> walkPred
        ) {
            assert root != null : "root";
            assert emitPred != null : "emitPredicate";
            assert walkPred != null : "walkPred";

            this.emitPred = emitPred;
            this.walkPred = walkPred;

            iterStack.add(new ChildrenIterator(root));
        }

        /**
         * @param root Root element.
         * @param emitPred Emit predicate. Used to decide whether the current
         *                 should be returned by {@link ASTNodeFinder#findNext()} or not.
         */
        ASTNodeFinder(
            GridSqlAst root,
            BiPredicate<GridSqlAst, GridSqlAst> emitPred
        ) {
            this.emitPred = emitPred;

            walkPred = child -> child instanceof GridSqlElement;

            iterStack.add(new ChildrenIterator(root));
        }

        /**
         * Returns next result that satisfies the {@link ASTNodeFinder#emitPred}
         * @return next result or {@code null}, if there are no more nodes.
         */
        Result findNext() {
            while (!iterStack.isEmpty()) {
                ChildrenIterator childrenIter = iterStack.get(iterStack.size() - 1);

                GridSqlAst curChild = childrenIter.nextChild();
                if (curChild == null)
                    iterStack.remove(iterStack.size() - 1);

                if (walkPred.test(curChild))
                    iterStack.add(new ChildrenIterator(curChild));

                if (emitPred.test(childrenIter.el, curChild))
                    return new Result(childrenIter.childInd, childrenIter.el);
            }

            return null;
        }
    }
}
