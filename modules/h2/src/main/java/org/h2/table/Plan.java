/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionVisitor;
import org.h2.message.Trace;
import org.h2.table.TableFilter.TableFilterVisitor;
import org.h2.util.New;

/**
 * A possible query execution plan. The time required to execute a query depends
 * on the order the tables are accessed.
 */
public class Plan {

    private final TableFilter[] filters;
    private final HashMap<TableFilter, PlanItem> planItems = new HashMap<>();
    private final Expression[] allConditions;
    private final TableFilter[] allFilters;

    /**
     * Create a query plan with the given order.
     *
     * @param filters the tables of the query
     * @param count the number of table items
     * @param condition the condition in the WHERE clause
     */
    public Plan(TableFilter[] filters, int count, Expression condition) {
        this.filters = new TableFilter[count];
        System.arraycopy(filters, 0, this.filters, 0, count);
        final ArrayList<Expression> allCond = New.arrayList();
        final ArrayList<TableFilter> all = New.arrayList();
        if (condition != null) {
            allCond.add(condition);
        }
        for (int i = 0; i < count; i++) {
            TableFilter f = filters[i];
            f.visit(new TableFilterVisitor() {
                @Override
                public void accept(TableFilter f) {
                    all.add(f);
                    if (f.getJoinCondition() != null) {
                        allCond.add(f.getJoinCondition());
                    }
                }
            });
        }
        allConditions = allCond.toArray(new Expression[0]);
        allFilters = all.toArray(new TableFilter[0]);
    }

    /**
     * Get the plan item for the given table.
     *
     * @param filter the table
     * @return the plan item
     */
    public PlanItem getItem(TableFilter filter) {
        return planItems.get(filter);
    }

    /**
     * The the list of tables.
     *
     * @return the list of tables
     */
    public TableFilter[] getFilters() {
        return filters;
    }

    /**
     * Remove all index conditions that can not be used.
     */
    public void removeUnusableIndexConditions() {
        for (int i = 0; i < allFilters.length; i++) {
            TableFilter f = allFilters[i];
            setEvaluatable(f, true);
            if (i < allFilters.length - 1 ||
                    f.getSession().getDatabase().getSettings().earlyFilter) {
                // the last table doesn't need the optimization,
                // otherwise the expression is calculated twice unnecessarily
                // (not that bad but not optimal)
                f.optimizeFullCondition(false);
            }
            f.removeUnusableIndexConditions();
        }
        for (TableFilter f : allFilters) {
            setEvaluatable(f, false);
        }
    }

    /**
     * Calculate the cost of this query plan.
     *
     * @param session the session
     * @return the cost
     */
    public double calculateCost(Session session) {
        Trace t = session.getTrace();
        if (t.isDebugEnabled()) {
            t.debug("Plan       : calculate cost for plan {0}", Arrays.toString(allFilters));
        }
        double cost = 1;
        boolean invalidPlan = false;
        final HashSet<Column> allColumnsSet = ExpressionVisitor
                .allColumnsForTableFilters(allFilters);
        for (int i = 0; i < allFilters.length; i++) {
            TableFilter tableFilter = allFilters[i];
            if (t.isDebugEnabled()) {
                t.debug("Plan       :   for table filter {0}", tableFilter);
            }
            PlanItem item = tableFilter.getBestPlanItem(session, allFilters, i, allColumnsSet);
            planItems.put(tableFilter, item);
            if (t.isDebugEnabled()) {
                t.debug("Plan       :   best plan item cost {0} index {1}",
                        item.cost, item.getIndex().getPlanSQL());
            }
            cost += cost * item.cost;
            setEvaluatable(tableFilter, true);
            Expression on = tableFilter.getJoinCondition();
            if (on != null) {
                if (!on.isEverything(ExpressionVisitor.EVALUATABLE_VISITOR)) {
                    invalidPlan = true;
                    break;
                }
            }
        }
        if (invalidPlan) {
            cost = Double.POSITIVE_INFINITY;
        }
        if (t.isDebugEnabled()) {
            session.getTrace().debug("Plan       : plan cost {0}", cost);
        }
        for (TableFilter f : allFilters) {
            setEvaluatable(f, false);
        }
        return cost;
    }

    private void setEvaluatable(TableFilter filter, boolean b) {
        filter.setEvaluatable(filter, b);
        for (Expression e : allConditions) {
            e.setEvaluatable(filter, b);
        }
    }
}
