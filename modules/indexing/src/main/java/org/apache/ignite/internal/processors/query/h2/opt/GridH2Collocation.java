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

package org.apache.ignite.internal.processors.query.h2.opt;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.h2.command.dml.Query;
import org.h2.command.dml.Select;
import org.h2.command.dml.SelectUnion;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.index.IndexCondition;
import org.h2.index.ViewIndex;
import org.h2.table.Column;
import org.h2.table.SubQueryInfo;
import org.h2.table.Table;
import org.h2.table.TableFilter;

/**
 * Collocation model for a query.
 */
public final class GridH2Collocation {
    /** */
    public static final int MULTIPLIER_COLLOCATED = 1;

    /** */
    private static final int MULTIPLIER_UNICAST = 20;

    /** */
    private static final int MULTIPLIER_BROADCAST = 80;

    /** */
    private final GridH2Collocation upper;

    /** */
    private final int filter;

    /** */
    private int multiplier;

    /** */
    private Type type;

    /** */
    private GridH2Collocation[] children;

    /** */
    private TableFilter[] childFilters;

    /** */
    private boolean childsOrderFinalized;

    /** */
    private List<GridH2Collocation> unions;

    /** */
    private Select select;

    /**
     * @param upper Upper.
     * @param filter Filter.
     */
    public GridH2Collocation(GridH2Collocation upper, int filter) {
        this.upper = upper;
        this.filter = filter;
    }

    /**
     * @return List of unions.
     */
    public List<GridH2Collocation> unions() {
        return unions;
    }

    /**
     * @param unions Unions.
     */
    public void unions(List<GridH2Collocation> unions) {
        this.unions = unions;
    }

    /**
     * @param childFilters New child filters.
     * @return {@code true} If child filters were updated.
     */
    private boolean childFilters(TableFilter[] childFilters) {
        assert childFilters != null;

        Select select = childFilters[0].getSelect();

        assert this.select == null || this.select == select;

        if (this.select == null) {
            this.select = select;

            assert this.childFilters == null;
        }
        else if (Arrays.equals(this.childFilters, childFilters))
            return false;

        childsOrderFinalized = false;

        if (this.childFilters == null) {
            // We have to clone because H2 reuses array and reorders elements.
            this.childFilters = childFilters.clone();

            children = new GridH2Collocation[childFilters.length];
        }
        else {
            assert this.childFilters.length == childFilters.length;

            // We have to copy because H2 reuses array and reorders elements.
            System.arraycopy(childFilters, 0, this.childFilters, 0, childFilters.length);

            Arrays.fill(children, null);
        }

        reset();

        return true;
    }

    /**
     * Reset current collocation model and all the children, but do not touch union.
     */
    private void reset() {
        type = null;
        multiplier = 0;
    }

    /**
     * @param i Index.
     * @param f Table filter.
     * @return {@code true} If the child is not a table or view.
     */
    private boolean isNotTableOrViewChild(int i, TableFilter f) {
        if (f == null)
            f = childFilters[i];

        Table t = f.getTable();

        return !t.isView() && !(t instanceof GridH2Table);
    }

    /**
     * Do the needed calculations.
     */
    private void calculate() {
        if (type != null)
            return;

        if (childFilters != null) {
            // We are at sub-query.
            boolean collocated = true;
            boolean partitioned = false;
            int maxMultiplier = 0;

            for (int i = 0; i < childFilters.length; i++) {
                GridH2Collocation c = child(i);

                if (c == null) {
                    assert isNotTableOrViewChild(i, null);

                    continue;
                }

                Type t = c.type(true);

                if (!t.isCollocated()) {
                    collocated = false;

                    int m = c.multiplier(true);

                    if (m > maxMultiplier)
                        maxMultiplier = m;
                }

                if (t.isPartitioned())
                    partitioned = true;
            }

            type = Type.of(partitioned, collocated);
            multiplier = type.isCollocated() ? MULTIPLIER_COLLOCATED : maxMultiplier;
        }
        else {
            assert upper != null;

            // We are at table instance.
            GridH2Table tbl = (GridH2Table)upper.childFilters[filter].getTable();

            // Only partitioned tables will do distributed joins.
            if (!tbl.isPartitioned()) {
                type = Type.REPLICATED;
                multiplier = MULTIPLIER_COLLOCATED;

                return;
            }

            // If we are the first partitioned table in a join, then we are "base" for all the rest partitioned tables
            // which will need to get remote result (if there is no affinity condition). Since this query is broadcasted
            // to all the affinity nodes the "base" does not need to get remote results.
            if (!upper.findPartitionedTableBefore(filter)) {
                type = Type.PARTITIONED_COLLOCATED;
                multiplier = MULTIPLIER_COLLOCATED;

                return;
            }

            // It is enough to make sure that our previous join by affinity key is collocated, then we are
            // collocated. If we at least have affinity key condition, then we do unicast which is cheaper.
            switch (upper.joinedWithCollocated(filter)) {
                case JOINED_WITH_COLLOCATED:
                    type = Type.PARTITIONED_COLLOCATED;
                    multiplier = MULTIPLIER_COLLOCATED;

                    break;

                case HAS_AFFINITY_CONDITION:
                    type = Type.PARTITIONED_NOT_COLLOCATED;
                    multiplier = MULTIPLIER_UNICAST;

                    break;

                case NONE:
                    type = Type.PARTITIONED_NOT_COLLOCATED;
                    multiplier = MULTIPLIER_BROADCAST;

                    break;
            }
        }
    }

    /**
     * @param f Current filter.
     * @return {@code true} If partitioned table was found.
     */
    private boolean findPartitionedTableBefore(int f) {
        for (int i = 0; i < f; i++) {
            GridH2Collocation c = child(i);

            assert c != null || isNotTableOrViewChild(i, null);

            // The `c` can be null if it is not a GridH2Table and not a sub-query,
            // it is a some kind of function table or anything else that considered replicated.
            if (c != null && c.type(true).isPartitioned())
                return true;
        }

        // We have to search globally in upper queries as well.
        return upper != null && upper.findPartitionedTableBefore(filter);
    }

    /**
     * @param f Filter.
     * @return Affinity join type.
     */
    private Affinity joinedWithCollocated(int f) {
        TableFilter tf = childFilters[f];

        ArrayList<IndexCondition> idxConditions = tf.getIndexConditions();

        int affColId = ((GridH2Table)tf.getTable()).getAffinityKeyColumnId();

        boolean affKeyConditionFound = false;

        for (int i = 0; i < idxConditions.size(); i++) {
            IndexCondition c = idxConditions.get(i);

            if (c.getCompareType() == IndexCondition.EQUALITY &&
                c.getColumn().getColumnId() == affColId && c.isEvaluatable()) {
                affKeyConditionFound = true;

                Expression exp = c.getExpression();

                exp = exp.getNonAliasExpression();

                if (exp instanceof ExpressionColumn) {
                    ExpressionColumn expCol = (ExpressionColumn)exp;

                    // This is one of our previous joins.
                    TableFilter prevJoin = expCol.getTableFilter();

                    if (prevJoin != null) {
                        GridH2Collocation co = child(indexOf(prevJoin));

                        assert co != null || isNotTableOrViewChild(-1, prevJoin);

                        if (co != null) {
                            Type t = co.type(true);

                            if (t.isPartitioned() && t.isCollocated() && isAffinityColumn(prevJoin, expCol))
                                return Affinity.JOINED_WITH_COLLOCATED;
                        }
                    }
                }
            }
        }

        return affKeyConditionFound ? Affinity.HAS_AFFINITY_CONDITION : Affinity.NONE;
    }

    /**
     * @param f Table filter.
     * @return Index.
     */
    private int indexOf(TableFilter f) {
        for (int i = 0; i < childFilters.length; i++) {
            if (childFilters[i] == f)
                return i;
        }

        throw new IllegalStateException();
    }

    /**
     * @param f Table filter.
     * @param expCol Expression column.
     * @return {@code true} It it is an affinity column.
     */
    private static boolean isAffinityColumn(TableFilter f, ExpressionColumn expCol) {
        Column col = expCol.getColumn();

        if (col == null)
            return false;

        Table t = col.getTable();

        if (t.isView()) {
            Query qry = ((ViewIndex)f.getIndex()).getQuery();

            return isAffinityColumn(qry, expCol);
        }

        return t instanceof GridH2Table &&
            col.getColumnId() == ((GridH2Table)t).getAffinityKeyColumnId();
    }

    /**
     * @param qry Query.
     * @param expCol Expression column.
     * @return {@code true} It it is an affinity column.
     */
    private static boolean isAffinityColumn(Query qry, ExpressionColumn expCol) {
        if (qry.isUnion()) {
            SelectUnion union = (SelectUnion)qry;

            return isAffinityColumn(union.getLeft(), expCol) && isAffinityColumn(union.getRight(), expCol);
        }

        Expression exp = qry.getExpressions().get(expCol.getColumn().getColumnId()).getNonAliasExpression();

        if (exp instanceof ExpressionColumn) {
            expCol = (ExpressionColumn)exp;

            return isAffinityColumn(expCol.getTableFilter(), expCol);
        }

        return false;
    }

    /**
     * Sets table filters to the final state of query.
     *
     * @return {@code false} if nothing was actually done here.
     */
    private boolean finalizeChildFiltersOrder() {
        if (childFilters == null || childsOrderFinalized)
            return false;

        int i = 0;

        // Collect table filters in final order after optimization.
        for (TableFilter f = select.getTopTableFilter(); f != null; f = f.getJoin()) {
            childFilters[i] = f;

            GridH2Collocation c = child(i);

            if (c == null)
                child(i, c = new GridH2Collocation(this, i));

            if (f.getTable().isView())
                c.finalizeChildFiltersOrder();

            i++;
        }

        assert i == childFilters.length;

        reset();

        childsOrderFinalized = true;

        return true;
    }

    /**
     * @return Multiplier.
     */
    public int calculateMultiplier() {
        if (childFilters != null && !childsOrderFinalized) {
            // We have to set all sub-queries structure to the final one we will have in query.
            boolean needReset = false;

            for (int i = 0; i < childFilters.length; i++) {
                Table t = childFilters[i].getTable();

                if (t.isView() || t instanceof GridH2Table) {
                    if (child(i) == null) {
                        child(i, new GridH2Collocation(this, i));

                        needReset = true;
                    }

                    if (t.isView() && child(i).finalizeChildFiltersOrder())
                        needReset = true;
                }
            }

            if (needReset)
                reset();

            childsOrderFinalized = true;
        }

        // We don't need multiplier for union here because it will be summarized in H2.
        return multiplier(false);
    }

    /**
     * @param withUnion With respect to union.
     * @return Multiplier.
     */
    private int multiplier(boolean withUnion) {
        calculate();

        assert multiplier != 0;

        if (withUnion && unions != null) {
            int maxMultiplier = unions.get(0).multiplier(false);

            for (int i = 1; i < unions.size(); i++) {
                int m = unions.get(i).multiplier(false);

                if (m > maxMultiplier)
                    maxMultiplier = m;
            }

            return maxMultiplier;
        }

        return multiplier;
    }

    /**
     * @param withUnion With respect to union.
     * @return Type.
     */
    private Type type(boolean withUnion) {
        calculate();

        assert type != null;

        if (withUnion && unions != null) {
            Type left = unions.get(0).type(false);

            for (int i = 1; i < unions.size(); i++) {
                Type right = unions.get(i).type(false);

                if (!left.isCollocated() || !right.isCollocated()) {
                    left = Type.PARTITIONED_NOT_COLLOCATED;

                    break;
                }
                else if (!left.isPartitioned() && !right.isPartitioned())
                    left = Type.REPLICATED;
                else
                    left = Type.PARTITIONED_COLLOCATED;
            }

            return left;
        }

        return type;
    }

    /**
     * @param idx Index.
     * @param child Child collocation.
     */
    private void child(int idx, GridH2Collocation child) {
        assert children[idx] == null;

        children[idx] = child;
    }

    /**
     * @param idx Index.
     * @return Child collocation.
     */
    private GridH2Collocation child(int idx) {
        return children[idx];
    }

    /**
     * @param qctx Query context.
     * @param info Sub-query info.
     * @param filters Filters.
     * @param filter Filter.
     * @return Collocation.
     */
    public static GridH2Collocation buildCollocationModel(GridH2QueryContext qctx, SubQueryInfo info,
        TableFilter[] filters, int filter) {
        GridH2Collocation c;

        if (info != null) {
            // Go up until we reach the root query.
            c = buildCollocationModel(qctx, info.getUpper(), info.getFilters(), info.getFilter());
        }
        else {
            // We are at the root query.
            c = qctx.queryCollocation();

            if (c == null) {
                c = new GridH2Collocation(null, -1);

                qctx.queryCollocation(c);
            }
        }

        Select select = filters[0].getSelect();

        // Handle union. We have to rely on fact that select will be the same on uppermost select.
        // For sub-queries we will drop collocation models, so that they will be recalculated anyways.
        if (c.select != null && c.select != select) {
            List<GridH2Collocation> unions = c.unions();

            int i = 1;

            if (unions == null) {
                unions = new ArrayList<>();

                unions.add(c);
                c.unions(unions);
            }
            else {
                for (; i < unions.size(); i++) {
                    GridH2Collocation u = unions.get(i);

                    if (u.select == select) {
                        c = u;

                        break;
                    }
                }
            }

            if (i == unions.size()) {
                c = new GridH2Collocation(c.upper, c.filter);

                unions.add(c);

                c.unions(unions);
            }
        }

        c.childFilters(filters);

        GridH2Collocation child = c.child(filter);

        if (child == null) {
            child = new GridH2Collocation(c, filter);

            c.child(filter, child);
        }

        return child;
    }

    /**
     * @param qry Query.
     * @return {@code true} If the query is collocated.
     */
    public static boolean isCollocated(Query qry) {
        return buildCollocationModel(null, -1, qry, null).type(true).isCollocated();
    }

    /**
     * @param upper Upper.
     * @param filter Filter.
     * @param qry Query.
     * @param unions Unions.
     * @return Built model.
     */
    private static GridH2Collocation buildCollocationModel(GridH2Collocation upper, int filter, Query qry,
        List<GridH2Collocation> unions) {
        if (qry.isUnion()) {
            if (unions == null)
                unions = new ArrayList<>();

            SelectUnion union = (SelectUnion)qry;

            GridH2Collocation a = buildCollocationModel(upper, filter, union.getLeft(), unions);
            GridH2Collocation b = buildCollocationModel(upper, filter, union.getRight(), unions);

            return a == null ? b : a;
        }

        Select select = (Select)qry;

        List<TableFilter> list = new ArrayList<>();

        for (TableFilter f = select.getTopTableFilter(); f != null; f = f.getJoin())
            list.add(f);

        TableFilter[] filters = list.toArray(new TableFilter[list.size()]);

        GridH2Collocation c = new GridH2Collocation(upper, filter);

        if (unions != null) {
            unions.add(c);

            c.unions(unions);
        }

        c.childFilters(filters);

        if (upper != null)
            upper.child(filter, c);

        for (int i = 0; i < filters.length; i++) {
            TableFilter f = filters[i];

            if (f.getTable().isView())
                c.child(i, buildCollocationModel(c, i, ((ViewIndex)f.getIndex()).getQuery(), null));
            else if (f.getTable() instanceof GridH2Table)
                c.child(i, new GridH2Collocation(c, i));
        }

        return upper == null ? c : null;
    }

    /**
     * Collocation type.
     */
    private enum Type {
        /** */
        PARTITIONED_COLLOCATED(true, true),

        /** */
        PARTITIONED_NOT_COLLOCATED(true, false),

        /** */
        REPLICATED(false, true);

        /** */
        private final boolean partitioned;

        /** */
        private final boolean collocated;

        /**
         * @param partitioned Partitioned.
         * @param collocated Collocated.
         */
        Type(boolean partitioned, boolean collocated) {
            this.partitioned = partitioned;
            this.collocated = collocated;
        }

        /**
         * @return {@code true} If partitioned.
         */
        public boolean isPartitioned() {
            return partitioned;
        }

        /**
         * @return {@code true} If collocated.
         */
        public boolean isCollocated() {
            return collocated;
        }

        /**
         * @param partitioned Partitioned.
         * @param collocated Collocated.
         * @return Type.
         */
        static Type of(boolean partitioned, boolean collocated) {
            if (collocated)
                return partitioned ? Type.PARTITIONED_COLLOCATED : Type.REPLICATED;

            assert partitioned;

            return Type.PARTITIONED_NOT_COLLOCATED;
        }
    }

    /**
     * Affinity of a table relative to previous joined tables.
     */
    private enum Affinity {
        /** */
        NONE,

        /** */
        HAS_AFFINITY_CONDITION,

        /** */
        JOINED_WITH_COLLOCATED
    }
}
