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

package org.apache.ignite.internal.processors.query.h2.opt.join;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.cache.CacheException;

import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQueryParser;
import org.apache.ignite.internal.processors.query.h2.sql.SplitterContext;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.h2.command.dml.Query;
import org.h2.command.dml.Select;
import org.h2.command.dml.SelectUnion;
import org.h2.engine.Session;
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
public final class CollocationModel {
    /** Empty filter array. */
    private static final TableFilter[] EMPTY_FILTERS = new TableFilter[0];

    /** */
    private final CollocationModel upper;

    /** */
    private final int filter;

    /** */
    private final boolean view;

    /** */
    private CollocationModelMultiplier multiplier;

    /** */
    private CollocationModelType type;

    /** */
    private CollocationModel[] children;

    /** */
    private TableFilter[] childFilters;

    /** */
    private List<CollocationModel> unions;

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
    private CollocationModel(CollocationModel upper, int filter, boolean view, boolean validate) {
        this.upper = upper;
        this.filter = filter;
        this.view = view;
        this.validate = validate;
    }

    /**
     * @return Table filter for this collocation model.
     */
    private TableFilter filter() {
        return upper == null ? null : upper.childFilters[filter];
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        calculate();

        SB b = new SB();

        for (int lvl = 0; lvl < 20; lvl++) {
            if (!toString(b, lvl))
                break;

            b.a('\n');
        }

        return b.toString();
    }

    /**
     * @param b String builder.
     * @param lvl Depth level.
     */
    private boolean toString(SB b, int lvl) {
        boolean res = false;

        if (lvl == 0) {
            TableFilter f = filter();
            String tblAlias = f == null ? "^" : f.getTableAlias();

            b.a("[tbl=").a(tblAlias).a(", type=").a(type).a(", mul=").a(multiplier).a("]");

            res = true;
        }
        else if (childFilters != null) {
            assert lvl > 0;

            lvl--;

            for (int i = 0; i < childFilters.length; i++) {
                if (lvl == 0)
                    b.a(" | ");

                res |= child(i, true).toString(b, lvl);
            }

            if (lvl == 0)
                b.a(" | ");
        }

        return res;
    }

    /**
     * @param upper Upper.
     * @param filter Filter.
     * @param unions Unions.
     * @param view This model will be a subquery (or top level query) and must contain child filters.
     * @param validate Query validation flag.
     * @return Created child collocation model.
     */
    private static CollocationModel createChildModel(CollocationModel upper,
        int filter,
        List<CollocationModel> unions,
        boolean view,
        boolean validate) {
        CollocationModel child = new CollocationModel(upper, filter, view, validate);

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

        if (this.childFilters == null || this.childFilters.length != childFilters.length) {
            // We have to clone because H2 reuses array and reorders elements.
            this.childFilters = childFilters.clone();

            children = new CollocationModel[childFilters.length];
        }
        else {
            // We have to copy because H2 reuses array and reorders elements.
            System.arraycopy(childFilters, 0, this.childFilters, 0, childFilters.length);

            Arrays.fill(children, null);
        }

        // Reset results.
        type = null;
        multiplier = null;

        return true;
    }

    /**
     * Do the needed calculations.
     */
    @SuppressWarnings("ConstantConditions")
    private void calculate() {
        if (type != null)
            return;

        if (view) { // We are at (sub-)query model.
            assert childFilters != null;

            boolean collocated = true;
            boolean partitioned = false;
            CollocationModelMultiplier maxMultiplier = CollocationModelMultiplier.COLLOCATED;

            for (int i = 0; i < childFilters.length; i++) {
                CollocationModel child = child(i, true);

                CollocationModelType t = child.type(true);

                if (child.multiplier == CollocationModelMultiplier.REPLICATED_NOT_LAST)
                    maxMultiplier = child.multiplier;

                if (t.isPartitioned()) {
                    partitioned = true;

                    if (!t.isCollocated()) {
                        collocated = false;

                        CollocationModelMultiplier m = child.multiplier(true);

                        if (m.multiplier() > maxMultiplier.multiplier()) {
                            maxMultiplier = m;

                            if (maxMultiplier == CollocationModelMultiplier.REPLICATED_NOT_LAST)
                                break;
                        }
                    }
                }
            }

            type = CollocationModelType.of(partitioned, collocated);
            multiplier = maxMultiplier;
        }
        else {
            assert upper != null;
            assert childFilters == null;

            // We are at table instance.
            Table tbl = filter().getTable();

            // Only partitioned tables will do distributed joins.
            if (!(tbl instanceof GridH2Table) || !((GridH2Table)tbl).isPartitioned()) {
                type = CollocationModelType.REPLICATED;
                multiplier = CollocationModelMultiplier.COLLOCATED;

                return;
            }

            // If we are the first partitioned table in a join, then we are "base" for all the rest partitioned tables
            // which will need to get remote result (if there is no affinity condition). Since this query is broadcasted
            // to all the affinity nodes the "base" does not need to get remote results.
            if (!upper.isPartitionedTableBeforeExists(filter)) {
                type = CollocationModelType.PARTITIONED_COLLOCATED;
                multiplier = CollocationModelMultiplier.COLLOCATED;
            }
            else {
                // It is enough to make sure that our previous join by affinity key is collocated, then we are
                // collocated. If we at least have affinity key condition, then we do unicast which is cheaper.
                switch (upper.joinedWithCollocated(filter)) {
                    case COLLOCATED_JOIN:
                        type = CollocationModelType.PARTITIONED_COLLOCATED;
                        multiplier = CollocationModelMultiplier.COLLOCATED;

                        break;

                    case HAS_AFFINITY_CONDITION:
                        type = CollocationModelType.PARTITIONED_NOT_COLLOCATED;
                        multiplier = CollocationModelMultiplier.UNICAST;

                        break;

                    case NONE:
                        type = CollocationModelType.PARTITIONED_NOT_COLLOCATED;
                        multiplier = CollocationModelMultiplier.BROADCAST;

                        break;

                    default:
                        throw new IllegalStateException();
                }
            }

            if (upper.isPreviousTableReplicated(filter))
                multiplier = CollocationModelMultiplier.REPLICATED_NOT_LAST;
        }
    }

    /**
     * Check whether at least one PARTITIONED table is located before current table.
     *
     * @param filterIdx Current filter index.
     * @return {@code true} If PARTITIONED table exists.
     */
    private boolean isPartitionedTableBeforeExists(int filterIdx) {
        for (int idx = 0; idx < filterIdx; idx++) {
            CollocationModel child = child(idx, true);

            // The c can be null if it is not a GridH2Table and not a sub-query,
            // it is a some kind of function table or anything else that considered replicated.
            if (child != null && child.type(true).isPartitioned())
                return true;
        }

        // We have to search globally in upper queries as well.
        return upper != null && upper.isPartitionedTableBeforeExists(filter);
    }

    /**
     * Check if previous table in the sequence is REPLICATED.
     *
     * @param filterIdx Current filter index.
     * @return {@code true} If previous table is REPLICATED.
     */
    private boolean isPreviousTableReplicated(int filterIdx) {
        // We are at the first table, nothing exists before it
        if (filterIdx == 0)
            return false;

        CollocationModel child = child(filterIdx - 1, true);

        if (child != null && child.type(true) == CollocationModelType.REPLICATED)
            return true;

        return upper != null && upper.isPreviousTableReplicated(filter);
    }

    /**
     * @param f Filter.
     * @return Affinity join type.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    private CollocationModelAffinity joinedWithCollocated(int f) {
        TableFilter tf = childFilters[f];

        GridH2Table tbl = (GridH2Table)tf.getTable();

        if (validate) {
            if (tbl.isCustomAffinityMapper())
                throw customAffinityError(tbl.cacheName());

            if (F.isEmpty(tf.getIndexConditions())) {
                throw new CacheException("Failed to prepare distributed join query: " +
                    "join condition does not use index [joinedCache=" + tbl.cacheName() +
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
                int colId = c.getColumn().getColumnId();
                int cmpType = c.getCompareType();

                if ((cmpType == Comparison.EQUAL || cmpType == Comparison.EQUAL_NULL_SAFE) &&
                    (colId == affColId || tbl.rowDescriptor().isKeyColumn(colId)) && c.isEvaluatable()) {
                    affKeyCondFound = true;

                    Expression exp = c.getExpression();
                    exp = exp.getNonAliasExpression();

                    if (exp instanceof ExpressionColumn) {
                        ExpressionColumn expCol = (ExpressionColumn)exp;

                        // This is one of our previous joins.
                        TableFilter prevJoin = expCol.getTableFilter();

                        if (prevJoin != null) {
                            CollocationModel cm = child(indexOf(prevJoin), true);

                            // If the previous joined model is a subquery (view), we can not be sure that
                            // the found affinity column is the needed one, since we can select multiple
                            // different affinity columns from different tables.
                            if (cm != null && !cm.view) {
                                CollocationModelType t = cm.type(true);

                                if (t.isPartitioned() && t.isCollocated() && isAffinityColumn(prevJoin, expCol, validate))
                                    return CollocationModelAffinity.COLLOCATED_JOIN;
                            }
                        }
                    }
                }
            }
        }

        return affKeyCondFound ? CollocationModelAffinity.HAS_AFFINITY_CONDITION : CollocationModelAffinity.NONE;
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
    @SuppressWarnings("IfMayBeConditional")
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
            GridH2Table t0 = (GridH2Table)t;

            if (validate && t0.isCustomAffinityMapper())
                throw customAffinityError((t0).cacheName());

            IndexColumn affCol = t0.getAffinityKeyColumn();

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
     * @param withUnion With respect to union.
     * @return Multiplier.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    private CollocationModelMultiplier multiplier(boolean withUnion) {
        calculate();

        assert multiplier != null;

        if (withUnion && unions != null) {
            CollocationModelMultiplier maxMultiplier = null;

            for (int i = 0; i < unions.size(); i++) {
                CollocationModelMultiplier m = unions.get(i).multiplier(false);

                if (maxMultiplier == null || m.multiplier() > maxMultiplier.multiplier())
                    maxMultiplier = m;
            }

            assert maxMultiplier != null;

            return maxMultiplier;
        }

        return multiplier;
    }

    /**
     * @param withUnion With respect to union.
     * @return Type.
     */
    private CollocationModelType type(boolean withUnion) {
        calculate();

        assert type != null;

        if (withUnion && unions != null) {
            CollocationModelType left = unions.get(0).type(false);

            for (int i = 1; i < unions.size(); i++) {
                CollocationModelType right = unions.get(i).type(false);

                if (!left.isCollocated() || !right.isCollocated()) {
                    left = CollocationModelType.PARTITIONED_NOT_COLLOCATED;

                    break;
                }
                else if (!left.isPartitioned() && !right.isPartitioned())
                    left = CollocationModelType.REPLICATED;
                else
                    left = CollocationModelType.PARTITIONED_COLLOCATED;
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
    @SuppressWarnings("IfMayBeConditional")
    private CollocationModel child(int i, boolean create) {
        CollocationModel child = children[i];

        if (child == null && create) {
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
    private List<CollocationModel> getOrCreateUnions() {
        if (unions == null) {
            unions = new ArrayList<>(4);

            unions.add(this);
        }

        return unions;
    }

    /**
     * Get distributed multiplier for the given sequence of tables.
     *
     * @param ses Session.
     * @param filters Filters.
     * @param filter Filter index.
     * @return Multiplier.
     */
    public static CollocationModelMultiplier distributedMultiplier(Session ses, TableFilter[] filters, int filter) {
        // Notice that we check for isJoinBatchEnabled, because we can do multiple different
        // optimization passes on PREPARE stage.
        // Query expressions can not be distributed as well.
        SplitterContext ctx = SplitterContext.get();

        if (!ctx.distributedJoins() || !ses.isJoinBatchEnabled() || ses.isPreparingQueryExpression())
            return CollocationModelMultiplier.COLLOCATED;

        assert filters != null;

        clearViewIndexCache(ses);

        CollocationModel model = buildCollocationModel(ctx, ses.getSubQueryInfo(), filters, filter, false);

        return model.multiplier(false);
    }

    /**
     * @param ctx Splitter context.
     * @param info Sub-query info.
     * @param filters Filters.
     * @param filter Filter.
     * @param validate Query validation flag.
     * @return Collocation.
     */
    private static CollocationModel buildCollocationModel(
        SplitterContext ctx,
        SubQueryInfo info,
        TableFilter[] filters,
        int filter,
        boolean validate
    ) {
        CollocationModel cm;

        if (info != null) {
            // Go up until we reach the root query.
            cm = buildCollocationModel(ctx, info.getUpper(), info.getFilters(), info.getFilter(), validate);
        }
        else {
            // We are at the root query.
            cm = ctx.collocationModel();

            if (cm == null) {
                cm = createChildModel(null, -1, null, true, validate);

                ctx.collocationModel(cm);
            }
        }

        if (filters == null)
            return cm;

        assert cm.view;

        Select select = filters[0].getSelect();

        // Handle union. We have to rely on fact that select will be the same on uppermost select.
        // For sub-queries we will drop collocation models, so that they will be recalculated anyways.
        if (cm.select != null && cm.select != select) {
            List<CollocationModel> unions = cm.getOrCreateUnions();

            // Try to find this select in existing unions.
            // Start with 1 because at 0 it always will be c.
            for (int i = 1; i < unions.size(); i++) {
                CollocationModel u = unions.get(i);

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
        CollocationModel mdl = buildCollocationModel(null, -1, qry, null, true);

        CollocationModelType type = mdl.type(true);

        if (!type.isCollocated() && mdl.multiplier == CollocationModelMultiplier.REPLICATED_NOT_LAST)
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
    private static CollocationModel buildCollocationModel(
        CollocationModel upper,
        int filter,
        Query qry,
        List<CollocationModel> unions,
        boolean validate
    ) {
        if (qry.isUnion()) {
            if (unions == null)
                unions = new ArrayList<>();

            SelectUnion union = (SelectUnion)qry;

            CollocationModel left = buildCollocationModel(upper, filter, union.getLeft(), unions, validate);
            CollocationModel right = buildCollocationModel(upper, filter, union.getRight(), unions, validate);

            assert left != null;
            assert right != null;

            return upper != null ? upper : left;
        }

        Select select = (Select)qry;

        List<TableFilter> list = new ArrayList<>();

        for (TableFilter f = select.getTopTableFilter(); f != null; f = f.getJoin())
            list.add(f);

        TableFilter[] filters = list.toArray(EMPTY_FILTERS);

        CollocationModel cm = createChildModel(upper, filter, unions, true, validate);

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
     * @param ses Session.
     */
    private static void clearViewIndexCache(Session ses) {
        // We have to clear this cache because normally sub-query plan cost does not depend on anything
        // other than index condition masks and sort order, but in our case it can depend on order
        // of previous table filters.
        Map<Object,ViewIndex> viewIdxCache = ses.getViewIndexCache(true);

        if (!viewIdxCache.isEmpty())
            viewIdxCache.clear();
    }
}
