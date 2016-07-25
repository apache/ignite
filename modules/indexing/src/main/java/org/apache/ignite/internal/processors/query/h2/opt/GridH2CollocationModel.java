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
import javax.cache.CacheException;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQueryParser;
import org.apache.ignite.internal.util.typedef.F;
import org.h2.command.dml.Query;
import org.h2.command.dml.Select;
import org.h2.command.dml.SelectUnion;
import org.h2.expression.Comparison;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.index.IndexCondition;
import org.h2.index.ViewIndex;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.SubQueryInfo;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.table.TableView;

/**
 * Collocation model for a query.
 */
public final class GridH2CollocationModel {
    /** */
    public static final int MULTIPLIER_COLLOCATED = 1;

    /** */
    private static final int MULTIPLIER_UNICAST = 50;

    /** */
    private static final int MULTIPLIER_BROADCAST = 200;

    /** */
    private static final int MULTIPLIER_REPLICATED_NOT_LAST = 10_000;

    /** */
    private final GridH2CollocationModel upper;

    /** */
    private final int filter;

    /** */
    private final boolean view;

    /** */
    private int multiplier;

    /** */
    private Type type;

    /** */
    private GridH2CollocationModel[] children;

    /** */
    private TableFilter[] childFilters;

    /** */
    private List<GridH2CollocationModel> unions;

    /** */
    private Select select;

    /** */
    private final boolean validate;

    /**
     * @param upper Upper.
     * @param filter Filter.
     * @param view This model will be a subquery (or top level query) and must contain child filters.
     * @param validate Query validation flag.
     */
    private GridH2CollocationModel(GridH2CollocationModel upper, int filter, boolean view, boolean validate) {
        this.upper = upper;
        this.filter = filter;
        this.view = view;
        this.validate = validate;
    }

    /**
     * @param upper Upper.
     * @param filter Filter.
     * @param unions Unions.
     * @param view This model will be a subquery (or top level query) and must contain child filters.
     * @param validate Query validation flag.
     * @return Created child collocation model.
     */
    private static GridH2CollocationModel createChildModel(GridH2CollocationModel upper,
        int filter,
        List<GridH2CollocationModel> unions,
        boolean view,
        boolean validate) {
        GridH2CollocationModel child = new GridH2CollocationModel(upper, filter, view, validate);

        if (unions != null) {
            // Bind created child to unions.
            assert upper == null || upper.child(filter, false) != null || unions.isEmpty();

            if (upper != null && unions.isEmpty()) {
                assert upper.child(filter, false) == null;

                upper.children[filter] = child;
            }

            unions.add(child);

            child.unions = unions;
        }
        else if (upper != null) {
            // Bind created child to upper model.
            assert upper.child(filter, false) == null;

            upper.children[filter] = child;
        }

        return child;
    }

    /**
     * @param childFilters New child filters.
     * @return {@code true} If child filters were updated.
     */
    private boolean childFilters(TableFilter[] childFilters) {
        assert childFilters != null;
        assert view;

        Select select = childFilters[0].getSelect();

        assert this.select == null || this.select == select;

        if (this.select == null) {
            this.select = select;

            assert this.childFilters == null;
        }
        else if (Arrays.equals(this.childFilters, childFilters))
            return false;

        if (this.childFilters == null) {
            // We have to clone because H2 reuses array and reorders elements.
            this.childFilters = childFilters.clone();

            children = new GridH2CollocationModel[childFilters.length];
        }
        else {
            assert this.childFilters.length == childFilters.length;

            // We have to copy because H2 reuses array and reorders elements.
            System.arraycopy(childFilters, 0, this.childFilters, 0, childFilters.length);

            Arrays.fill(children, null);
        }

        // Reset results.
        type = null;
        multiplier = 0;

        return true;
    }

    /**
     * @param i Index.
     * @param f Table filter.
     * @return {@code true} If the child is not a table or view.
     */
    private boolean isChildTableOrView(int i, TableFilter f) {
        if (f == null)
            f = childFilters[i];

        Table t = f.getTable();

        return t.isView() || t instanceof GridH2Table;
    }

    /**
     * Do the needed calculations.
     */
    private void calculate() {
        if (type != null)
            return;

        if (view) { // We are at (sub-)query model.
            assert childFilters != null;

            boolean collocated = true;
            boolean partitioned = false;
            int maxMultiplier = MULTIPLIER_COLLOCATED;

            for (int i = 0; i < childFilters.length; i++) {
                GridH2CollocationModel child = child(i, true);

                Type t = child.type(true);

                if (child.multiplier == MULTIPLIER_REPLICATED_NOT_LAST)
                    maxMultiplier = child.multiplier;

                if (t.isPartitioned()) {
                    partitioned = true;

                    if (!t.isCollocated()) {
                        collocated = false;

                        int m = child.multiplier(true);

                        if (m > maxMultiplier) {
                            maxMultiplier = m;

                            if (maxMultiplier == MULTIPLIER_REPLICATED_NOT_LAST)
                                break;
                        }
                    }
                }
            }

            type = Type.of(partitioned, collocated);
            multiplier = maxMultiplier;
        }
        else {
            assert upper != null;
            assert childFilters == null;

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
            }
            else {
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

                    default:
                        throw new IllegalStateException();
                }
            }

            if (upper.previousReplicated(filter))
                multiplier = MULTIPLIER_REPLICATED_NOT_LAST;
        }
    }

    /**
     * @param f Current filter.
     * @return {@code true} If partitioned table was found.
     */
    private boolean findPartitionedTableBefore(int f) {
        for (int i = 0; i < f; i++) {
            GridH2CollocationModel child = child(i, true);

            // The c can be null if it is not a GridH2Table and not a sub-query,
            // it is a some kind of function table or anything else that considered replicated.
            if (child != null && child.type(true).isPartitioned())
                return true;
        }

        // We have to search globally in upper queries as well.
        return upper != null && upper.findPartitionedTableBefore(filter);
    }

    /**
     * @param f Current filter.
     * @return {@code true} If previous table is REPLICATED.
     */
    private boolean previousReplicated(int f) {
        if (f > 0 && child(f - 1, true).type(true) == Type.REPLICATED)
            return true;

        return upper != null && upper.previousReplicated(filter);
    }

    /**
     * @param f Filter.
     * @return Affinity join type.
     */
    private Affinity joinedWithCollocated(int f) {
        TableFilter tf = childFilters[f];

        GridH2Table tbl = (GridH2Table)tf.getTable();

        if (validate) {
            if (tbl.rowDescriptor().context().customAffinityMapper())
                throw customAffinityError(tbl.spaceName());

            if (F.isEmpty(tf.getIndexConditions())) {
                throw new CacheException("Failed to prepare distributed join query: " +
                    "join condition does not use index [joinedCache=" + tbl.spaceName() +
                    ", plan=" + tf.getSelect().getPlanSQL() + ']');
            }
        }

        IndexColumn affCol = tbl.getAffinityKeyColumn();

        boolean affKeyCondFound = false;

        if (affCol != null) {
            ArrayList<IndexCondition> idxConditions = tf.getIndexConditions();

            int affColId = affCol.column.getColumnId();

            for (int i = 0; i < idxConditions.size(); i++) {
                IndexCondition c = idxConditions.get(i);

                int cmpType = c.getCompareType();

                if ((cmpType == Comparison.EQUAL || cmpType == Comparison.EQUAL_NULL_SAFE) &&
                    c.getColumn().getColumnId() == affColId && c.isEvaluatable()) {
                    affKeyCondFound = true;

                    Expression exp = c.getExpression();

                    exp = exp.getNonAliasExpression();

                    if (exp instanceof ExpressionColumn) {
                        ExpressionColumn expCol = (ExpressionColumn)exp;

                        // This is one of our previous joins.
                        TableFilter prevJoin = expCol.getTableFilter();

                        if (prevJoin != null) {
                            GridH2CollocationModel cm = child(indexOf(prevJoin), true);

                            if (cm != null) {
                                Type t = cm.type(true);

                                if (t.isPartitioned() && t.isCollocated() && isAffinityColumn(prevJoin, expCol, validate))
                                    return Affinity.JOINED_WITH_COLLOCATED;
                            }
                        }
                    }
                }
            }
        }

        return affKeyCondFound ? Affinity.HAS_AFFINITY_CONDITION : Affinity.NONE;
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
     * @param validate Query validation flag.
     * @return {@code true} It it is an affinity column.
     */
    private static boolean isAffinityColumn(TableFilter f, ExpressionColumn expCol, boolean validate) {
        Column col = expCol.getColumn();

        if (col == null)
            return false;

        Table t = col.getTable();

        if (t.isView()) {
            Query qry;

            if (f.getIndex() != null)
                qry = getSubQuery(f);
            else
                qry = GridSqlQueryParser.VIEW_QUERY.get((TableView)t);

            return isAffinityColumn(qry, expCol, validate);
        }

        if (t instanceof GridH2Table) {
            if (validate && ((GridH2Table)t).rowDescriptor().context().customAffinityMapper())
                throw customAffinityError(((GridH2Table)t).spaceName());

            IndexColumn affCol = ((GridH2Table)t).getAffinityKeyColumn();

            return affCol != null && col.getColumnId() == affCol.column.getColumnId();
        }

        return false;
    }

    /**
     * @param qry Query.
     * @param expCol Expression column.
     * @param validate Query validation flag.
     * @return {@code true} It it is an affinity column.
     */
    private static boolean isAffinityColumn(Query qry, ExpressionColumn expCol, boolean validate) {
        if (qry.isUnion()) {
            SelectUnion union = (SelectUnion)qry;

            return isAffinityColumn(union.getLeft(), expCol, validate) && isAffinityColumn(union.getRight(), expCol, validate);
        }

        Expression exp = qry.getExpressions().get(expCol.getColumn().getColumnId()).getNonAliasExpression();

        if (exp instanceof ExpressionColumn) {
            expCol = (ExpressionColumn)exp;

            return isAffinityColumn(expCol.getTableFilter(), expCol, validate);
        }

        return false;
    }

    /**
     * @return Multiplier.
     */
    public int calculateMultiplier() {
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
            int maxMultiplier = 0;

            for (int i = 0; i < unions.size(); i++) {
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
     * @param i Index.
     * @param create Create child if needed.
     * @return Child collocation.
     */
    private GridH2CollocationModel child(int i, boolean create) {
        GridH2CollocationModel child = children[i];

        if (child == null && create && isChildTableOrView(i, null)) {
            TableFilter f = childFilters[i];

            if (f.getTable().isView()) {
                if (f.getIndex() == null) {
                    // If we don't have view index yet, then we just creating empty model and it must be filled later.
                    child = createChildModel(this, i, null, true, validate);
                }
                else
                    child = buildCollocationModel(this, i, getSubQuery(f), null, validate);
            }
            else
                child = createChildModel(this, i, null, false, validate);

            assert child != null;
            assert children[i] == child;
        }

        return child;
    }

    /**
     * @param f Table filter.
     * @return Sub-query.
     */
    private static Query getSubQuery(TableFilter f) {
        return ((ViewIndex)f.getIndex()).getQuery();
    }

    /**
     * @return Unions list.
     */
    private List<GridH2CollocationModel> getOrCreateUnions() {
        if (unions == null) {
            unions = new ArrayList<>(4);

            unions.add(this);
        }

        return unions;
    }

    /**
     * @param qctx Query context.
     * @param info Sub-query info.
     * @param filters Filters.
     * @param filter Filter.
     * @param validate Query validation flag.
     * @return Collocation.
     */
    public static GridH2CollocationModel buildCollocationModel(GridH2QueryContext qctx, SubQueryInfo info,
        TableFilter[] filters, int filter, boolean validate) {
        GridH2CollocationModel cm;

        if (info != null) {
            // Go up until we reach the root query.
            cm = buildCollocationModel(qctx, info.getUpper(), info.getFilters(), info.getFilter(), validate);
        }
        else {
            // We are at the root query.
            cm = qctx.queryCollocationModel();

            if (cm == null) {
                cm = createChildModel(null, -1, null, true, validate);

                qctx.queryCollocationModel(cm);
            }
        }

        assert cm.view;

        Select select = filters[0].getSelect();

        // Handle union. We have to rely on fact that select will be the same on uppermost select.
        // For sub-queries we will drop collocation models, so that they will be recalculated anyways.
        if (cm.select != null && cm.select != select) {
            List<GridH2CollocationModel> unions = cm.getOrCreateUnions();

            // Try to find this select in existing unions.
            // Start with 1 because at 0 it always will be c.
            for (int i = 1; i < unions.size(); i++) {
                GridH2CollocationModel u = unions.get(i);

                if (u.select == select) {
                    cm = u;

                    break;
                }
            }

            // Nothing was found, need to create new child in union.
            if (cm.select != select)
                cm = createChildModel(cm.upper, cm.filter, unions, true, validate);
        }

        cm.childFilters(filters);

        return cm.child(filter, true);
    }

    /**
     * @param qry Query.
     * @return {@code true} If the query is collocated.
     */
    public static boolean isCollocated(Query qry) {
        GridH2CollocationModel mdl = buildCollocationModel(null, -1, qry, null, true);

        Type type = mdl.type(true);

        if (!type.isCollocated() && mdl.multiplier == MULTIPLIER_REPLICATED_NOT_LAST)
            throw new CacheException("Failed to execute query: for distributed join " +
                "all REPLICATED caches must be at the end of the joined tables list.");

        return type.isCollocated();
    }

    /**
     * @param upper Upper.
     * @param filter Filter.
     * @param qry Query.
     * @param unions Unions.
     * @param validate Query validation flag.
     * @return Built model.
     */
    private static GridH2CollocationModel buildCollocationModel(GridH2CollocationModel upper,
        int filter,
        Query qry,
        List<GridH2CollocationModel> unions,
        boolean validate) {
        if (qry.isUnion()) {
            if (unions == null)
                unions = new ArrayList<>();

            SelectUnion union = (SelectUnion)qry;

            GridH2CollocationModel left = buildCollocationModel(upper, filter, union.getLeft(), unions, validate);
            GridH2CollocationModel right = buildCollocationModel(upper, filter, union.getRight(), unions, validate);

            assert left != null;
            assert right != null;

            return upper != null ? upper : left;
        }

        Select select = (Select)qry;

        List<TableFilter> list = new ArrayList<>();

        for (TableFilter f = select.getTopTableFilter(); f != null; f = f.getJoin())
            list.add(f);

        TableFilter[] filters = list.toArray(new TableFilter[list.size()]);

        GridH2CollocationModel cm = createChildModel(upper, filter, unions, true, validate);

        cm.childFilters(filters);

        for (int i = 0; i < filters.length; i++) {
            TableFilter f = filters[i];

            if (f.getTable().isView())
                buildCollocationModel(cm, i, getSubQuery(f), null, validate);
            else if (f.getTable() instanceof GridH2Table)
                createChildModel(cm, i, null, false, validate);
        }

        return upper != null ? upper : cm;
    }

    /**
     * @param cacheName Cache name.
     * @return Error.
     */
    private static CacheException customAffinityError(String cacheName) {
        return new CacheException("Failed to prepare distributed join query: can not use distributed joins for cache " +
            "with custom AffinityKeyMapper configured. " +
            "Please use AffinityKeyMapped annotation instead [cache=" + cacheName + ']');
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
