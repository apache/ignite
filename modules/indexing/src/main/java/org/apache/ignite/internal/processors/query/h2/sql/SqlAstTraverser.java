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

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.jetbrains.annotations.Nullable;

/**
 * Traverse over query AST to find info about partitioned table usage.
 */
class SqlAstTraverser {
    /** Query AST root to check. */
    private final GridSqlAst root;

    /** Whether user specified distributed joins flag. */
    private final boolean distributedJoins;

    /** Ignite logger. */
    private final IgniteLogger log;

    /** Whether query has partitioned tables. */
    private boolean hasPartitionedTables;

    /** Whether query has sub queries. */
    private boolean hasSubQueries;

    /** Whether query has joins between replicated and partitioned tables. */
    private boolean hasOuterJoinReplicatedPartitioned;

    /** */
    private @Nullable MixedModeCachesJoinIssue hasOuterJoinMixedCacheModeIssue;

    /** Whether top-level table is replicated. */
    private boolean isRootTableReplicated;

    /** */
    SqlAstTraverser(GridSqlAst root, boolean distributedJoins, IgniteLogger log) {
        this.root = root;
        this.distributedJoins = distributedJoins;
        this.log = log;
    }

    /** */
    public void traverse() {
        if (root instanceof GridSqlSelect) {
            GridSqlTable table = getTable(((GridSqlSelect)root).from().child());

            if (table != null && !table.dataTable().isPartitioned())
                isRootTableReplicated = true;
        }

        lookForPartitionedJoin(root, null);
    }

    /** */
    public boolean hasPartitionedTables() {
        return hasPartitionedTables;
    }

    /** */
    public boolean hasSubQueries() {
        return hasSubQueries;
    }

    /** */
    public boolean hasOuterJoinReplicatedPartitioned() {
        return hasOuterJoinReplicatedPartitioned;
    }

    /** */
    public boolean hasReplicatedWithPartitionedAndSubQuery() {
        return (isRootTableReplicated && hasSubQueries && hasPartitionedTables);
    }

    /** */
    public @Nullable MixedModeCachesJoinIssue hasOuterJoinMixedCacheModeIssue() {
        return hasOuterJoinMixedCacheModeIssue;
    }

    /**
     * Traverse AST while join operation isn't found. Check it if found.
     *
     * @param ast AST item to check recursively.
     * @param upWhere Where condition that applies to this ast.
     */
    private void lookForPartitionedJoin(GridSqlAst ast, GridSqlAst upWhere) {
        if (ast == null)
            return;

        GridSqlJoin join = null;
        GridSqlAst where = null;

        if (ast instanceof GridSqlJoin) {
            join = (GridSqlJoin)ast;
            where = upWhere;
        }
        else if (ast instanceof GridSqlSelect) {
            GridSqlSelect select = (GridSqlSelect)ast;

            if (select.from() instanceof GridSqlJoin) {
                join = (GridSqlJoin)select.from();
                where = select.where();
            }
        }
        else if (ast instanceof GridSqlSubquery)
            hasSubQueries = true;
        else if (ast instanceof GridSqlTable)
            hasPartitionedTables |= ((GridSqlTable)ast).dataTable().isPartitioned();

        // No joins on this level. Traverse AST deeper.
        if (join == null) {
            for (int i = 0; i < ast.size(); i++)
                lookForPartitionedJoin(ast.child(i), null);

            return;
        }

        // Check WHERE clause first.
        lookForPartitionedJoin(where, null);

        // Check left side of join.
        GridSqlTable leftTable = getTable(join.leftTable());

        GridH2Table left = null;

        // Left side of join is a subquery.
        if (leftTable == null) {
            hasSubQueries = true;

            // Check subquery on left side.
            lookForPartitionedJoin(join.leftTable(), where);
        }
        else {
            left = leftTable.dataTable();

            // Data table is NULL for views.
            if (left != null && left.isPartitioned())
                hasPartitionedTables = true;
        }

        // Check right side of join.
        GridSqlTable rightTable = getTable(join.rightTable());

        // Right side of join is a subquery.
        if (rightTable == null) {
            hasSubQueries = true;

            // Check subquery and return (can't exctract more info there).
            lookForPartitionedJoin(join.rightTable(), where);
            return;
        }

        GridH2Table right = rightTable.dataTable();

        if (right != null && right.isPartitioned())
            hasPartitionedTables = true;

        // Skip check of views.
        if (left == null || right == null)
            return;

        if (join.isLeftOuter() && !left.isPartitioned() && right.isPartitioned()) {
            if (left.cacheContext().affinity().partitions() != right.cacheContext().affinity().partitions()) {
                hasOuterJoinMixedCacheModeIssue = new MixedModeCachesJoinIssue("Cache [cacheName=" + left.cacheName() +
                        ", partitionsCount=" + left.cacheContext().affinity().partitions() +
                        "] can`t be joined with [cacheName=" + right.cacheName() +
                        ", partitionsCount=" + right.cacheContext().affinity().partitions() +
                        "] due to different affinity configuration. Join between PARTITIONED and REPLICATED caches is possible "
                        + "only with the same partitions number configuration.");
            }
            // the only way to compare predicate classes, not work for different class loaders.
            else if (!Objects.equals(className(left.cacheInfo().config().getNodeFilter()), className(right.cacheInfo().config()
                    .getNodeFilter()))) {
                hasOuterJoinMixedCacheModeIssue = new MixedModeCachesJoinIssue("Cache [cacheName=" + left.cacheName() + "] "
                        + "can`t be joined with [cacheName=" + right.cacheName() + "] due to different node filters configuration.");
            }
            else
                hasOuterJoinReplicatedPartitioned = true;
        }

        // Skip check if at least one of tables isn't partitioned.
        if (!(left.isPartitioned() && right.isPartitioned()))
            return;

        if (!distributedJoins)
            checkPartitionedJoin(join, where, left, right, log);
    }

    /** Object class name. */
    @Nullable private static String className(@Nullable Object obj) {
        return obj != null ? obj.getClass().getName() : null;
    }

    /**
     * Checks whether an AST contains valid join operation between partitioned tables.
     * Join condition should be an equality operation of affinity keys of tables. Conditions can be splitted between
     * join and where clauses. If join is invalid then warning a user about that.
     *
     * @param join The join to check.
     * @param where The where statement from previous AST, for nested joins.
     * @param left Left side of join.
     * @param right Right side of join.
     * @param log Ignite logger.
     */
    private void checkPartitionedJoin(GridSqlJoin join, GridSqlAst where, GridH2Table left, GridH2Table right, IgniteLogger log) {
        String leftTblAls = getAlias(join.leftTable());
        String rightTblAls = getAlias(join.rightTable());

        // User explicitly specify an affinity key. Otherwise use primary key.
        boolean pkLeft = left.getExplicitAffinityKeyColumn() == null;
        boolean pkRight = right.getExplicitAffinityKeyColumn() == null;

        Set<String> leftAffKeys = affKeys(pkLeft, left);
        Set<String> rightAffKeys = affKeys(pkRight, right);

        boolean joinIsValid = checkPartitionedCondition(join.on(),
            leftTblAls, leftAffKeys, pkLeft,
            rightTblAls, rightAffKeys, pkRight);

        if (!joinIsValid && where instanceof GridSqlElement)
            joinIsValid = checkPartitionedCondition((GridSqlElement)where,
                leftTblAls, leftAffKeys, pkLeft,
                rightTblAls, rightAffKeys, pkRight);

        if (!joinIsValid) {
            log.warning(
                String.format(
                    "For join two partitioned tables join condition should contain the equality operation of affinity keys." +
                        " Left side: %s; right side: %s", left.getName(), right.getName())
            );
        }
    }

    /** Extract table instance from an AST element. */
    private GridSqlTable getTable(GridSqlElement el) {
        if (el instanceof GridSqlTable)
            return (GridSqlTable)el;

        if (el instanceof GridSqlAlias && el.child() instanceof GridSqlTable)
            return el.child();

        return null;
    }

    /** Extract alias value. */
    private String getAlias(GridSqlElement el) {
        if (el instanceof GridSqlAlias)
            return ((GridSqlAlias)el).alias();

        return null;
    }

    /** @return Set of possible affinity keys for this table, incl. default _KEY. */
    private Set<String> affKeys(boolean pk, GridH2Table tbl) {
        Set<String> affKeys = new HashSet<>();

        // User explicitly specify an affinity key. Otherwise, use primary key.
        if (!pk)
            affKeys.add(tbl.getAffinityKeyColumn().columnName);
        else {
            affKeys.add("_KEY");

            String keyFieldName = tbl.rowDescriptor().type().keyFieldName();

            if (keyFieldName == null)
                affKeys.addAll(tbl.rowDescriptor().type().primaryKeyFields());
            else
                affKeys.add(keyFieldName);
        }

        return affKeys;
    }

    /**
     * Valid join condition contains:
     * 1. Equality of Primary (incl. cases of complex PK) or Affinity keys;
     * 2. Additional conditions must be joint with AND operation to the affinity join condition.
     *
     * @return {@code true} if join condition contains affinity join condition, otherwise {@code false}.
     */
    private boolean checkPartitionedCondition(GridSqlElement condition,
        String leftTbl, Set<String> leftAffKeys, boolean pkLeft,
        String rightTbl, Set<String> rightAffKeys, boolean pkRight) {

        if (!(condition instanceof GridSqlOperation))
            return false;

        GridSqlOperation op = (GridSqlOperation)condition;

        // It is may be a part of affinity condition.
        if (GridSqlOperationType.EQUAL == op.operationType())
            checkEqualityOperation(op, leftTbl, leftAffKeys, pkLeft, rightTbl, rightAffKeys, pkRight);

        // Check affinity condition is covered fully. If true then return. Otherwise, go deeper.
        if (affinityCondIsCovered(leftAffKeys, rightAffKeys))
            return true;

        // If we don't cover affinity condition prior to first AND then this is not an affinity condition.
        if (GridSqlOperationType.AND != op.operationType())
            return false;

        // Go recursively to childs.
        for (int i = 0; i < op.size(); i++) {
            boolean ret = checkPartitionedCondition(op.child(i),
                leftTbl, leftAffKeys, pkLeft,
                rightTbl, rightAffKeys, pkRight);

            if (ret)
                return true;
        }

        // Join condition doesn't contain affinity condition.
        return false;
    }

    /** */
    private void checkEqualityOperation(GridSqlOperation equalOp,
        String leftTbl, Set<String> leftCols, boolean pkLeft,
        String rightTbl, Set<String> rightCols, boolean pkRight) {

        if (!(equalOp.child(0) instanceof GridSqlColumn))
            return;

        if (!(equalOp.child(1) instanceof GridSqlColumn))
            return;

        String leftTblAls = ((GridSqlColumn)equalOp.child(0)).tableAlias();
        String rightTblAls = ((GridSqlColumn)equalOp.child(1)).tableAlias();

        int leftColIdx = leftTbl.equals(leftTblAls) ? 0 : leftTbl.equals(rightTblAls) ? 1 : -1;
        int rightColIdx = rightTbl.equals(rightTblAls) ? 1 : rightTbl.equals(leftTblAls) ? 0 : -1;

        if (leftColIdx == -1 || rightColIdx == -1)
            return;

        String leftCol = ((GridSqlColumn)equalOp.child(leftColIdx)).columnName();
        String rightCol = ((GridSqlColumn)equalOp.child(rightColIdx)).columnName();

        // This is part of the affinity join condition.
        if (leftCols.contains(leftCol) && rightCols.contains(rightCol)) {
            leftCols.remove(leftCol);
            rightCols.remove(rightCol);

            // Only _KEY is there.
            if (pkLeft && (leftCols.size() == 1 || "_KEY".equals(leftCol)))
                leftCols.clear();

            if (pkRight && (rightCols.size() == 1 || "_KEY".equals(rightCol)))
                rightCols.clear();
        }
    }

    /** */
    private boolean affinityCondIsCovered(Set<String> leftAffKeys, Set<String> rightAffKeys) {
        return leftAffKeys.isEmpty() && rightAffKeys.isEmpty();
    }

    /** Mixed cache mode join issues. */
    static class MixedModeCachesJoinIssue {
        /** */
        private final boolean err;

        /** */
        private final String msg;

        /** Constructor. */
        MixedModeCachesJoinIssue(String errMsg) {
            err = true;
            msg = errMsg;
        }

        /** Return {@code true} if error present. */
        boolean error() {
            return err;
        }

        /** Return appropriate error message. */
        String errorMessage() {
            return msg;
        }
    }
}
