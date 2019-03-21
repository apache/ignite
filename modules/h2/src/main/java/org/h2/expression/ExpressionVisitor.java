/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import java.util.HashSet;
import org.h2.command.dml.AllColumnsForPlan;
import org.h2.engine.DbObject;
import org.h2.table.Column;
import org.h2.table.ColumnResolver;
import org.h2.table.Table;
import org.h2.table.TableFilter;

/**
 * The visitor pattern is used to iterate through all expressions of a query
 * to optimize a statement.
 */
public class ExpressionVisitor {

    /**
     * Is the value independent on unset parameters or on columns of a higher
     * level query, or sequence values (that means can it be evaluated right
     * now)?
     */
    public static final int INDEPENDENT = 0;

    /**
     * The visitor singleton for the type INDEPENDENT.
     */
    public static final ExpressionVisitor INDEPENDENT_VISITOR =
            new ExpressionVisitor(INDEPENDENT);

    /**
     * Are all aggregates MIN(column), MAX(column), COUNT(*), MEDIAN(column),
     * ENVELOPE(count) for the given table (getTable)?
     */
    public static final int OPTIMIZABLE_AGGREGATE = 1;

    /**
     * Does the expression return the same results for the same parameters?
     */
    public static final int DETERMINISTIC = 2;

    /**
     * The visitor singleton for the type DETERMINISTIC.
     */
    public static final ExpressionVisitor DETERMINISTIC_VISITOR =
            new ExpressionVisitor(DETERMINISTIC);

    /**
     * Can the expression be evaluated, that means are all columns set to
     * 'evaluatable'?
     */
    public static final int EVALUATABLE = 3;

    /**
     * The visitor singleton for the type EVALUATABLE.
     */
    public static final ExpressionVisitor EVALUATABLE_VISITOR =
            new ExpressionVisitor(EVALUATABLE);

    /**
     * Count of cached INDEPENDENT and EVALUATABLE visitors with different query
     * level.
     */
    private static final int CACHED = 8;

    /**
     * INDEPENDENT listeners with query level 0, 1, ...
     */
    private static final ExpressionVisitor[] INDEPENDENT_VISITORS;

    /**
     * EVALUATABLE listeners with query level 0, 1, ...
     */
    private static final ExpressionVisitor[] EVALUATABLE_VISITORS;

    static {
        ExpressionVisitor[] a = new ExpressionVisitor[CACHED];
        a[0] = INDEPENDENT_VISITOR;
        for (int i = 1; i < CACHED; i++) {
            a[i] = new ExpressionVisitor(INDEPENDENT, i);
        }
        INDEPENDENT_VISITORS = a;
        a = new ExpressionVisitor[CACHED];
        a[0] = EVALUATABLE_VISITOR;
        for (int i = 1; i < CACHED; i++) {
            a[i] = new ExpressionVisitor(EVALUATABLE, i);
        }
        EVALUATABLE_VISITORS = a;
    }

    /**
     * Request to set the latest modification id (addDataModificationId).
     */
    public static final int SET_MAX_DATA_MODIFICATION_ID = 4;

    /**
     * Does the expression have no side effects (change the data)?
     */
    public static final int READONLY = 5;

    /**
     * The visitor singleton for the type EVALUATABLE.
     */
    public static final ExpressionVisitor READONLY_VISITOR =
            new ExpressionVisitor(READONLY);

    /**
     * Does an expression have no relation to the given table filter
     * (getResolver)?
     */
    public static final int NOT_FROM_RESOLVER = 6;

    /**
     * Request to get the set of dependencies (addDependency).
     */
    public static final int GET_DEPENDENCIES = 7;

    /**
     * Can the expression be added to a condition of an outer query. Example:
     * ROWNUM() can't be added as a condition to the inner query of select id
     * from (select t.*, rownum as r from test t) where r between 2 and 3; Also
     * a sequence expression must not be used.
     */
    public static final int QUERY_COMPARABLE = 8;

    /**
     * Get all referenced columns for the optimiser.
     */
    public static final int GET_COLUMNS1 = 9;

    /**
     * Get all referenced columns.
     */
    public static final int GET_COLUMNS2 = 10;

    /**
     * The visitor singleton for the type QUERY_COMPARABLE.
     */
    public static final ExpressionVisitor QUERY_COMPARABLE_VISITOR =
            new ExpressionVisitor(QUERY_COMPARABLE);

    private final int type;
    private final int queryLevel;
    private final HashSet<DbObject> dependencies;
    private final AllColumnsForPlan columns1;
    private final Table table;
    private final long[] maxDataModificationId;
    private final ColumnResolver resolver;
    private final HashSet<Column> columns2;

    private ExpressionVisitor(int type,
            int queryLevel,
            HashSet<DbObject> dependencies,
            AllColumnsForPlan columns1, Table table, ColumnResolver resolver,
            long[] maxDataModificationId,
            HashSet<Column> columns2) {
        this.type = type;
        this.queryLevel = queryLevel;
        this.dependencies = dependencies;
        this.columns1 = columns1;
        this.table = table;
        this.resolver = resolver;
        this.maxDataModificationId = maxDataModificationId;
        this.columns2 = columns2;
    }

    private ExpressionVisitor(int type) {
        this.type = type;
        this.queryLevel = 0;
        this.dependencies = null;
        this.columns1 = null;
        this.columns2 = null;
        this.table = null;
        this.resolver = null;
        this.maxDataModificationId = null;
    }

    private ExpressionVisitor(int type, int queryLevel) {
        this.type = type;
        this.queryLevel = queryLevel;
        this.dependencies = null;
        this.columns1 = null;
        this.columns2 = null;
        this.table = null;
        this.resolver = null;
        this.maxDataModificationId = null;
    }

    /**
     * Create a new visitor object to collect dependencies.
     *
     * @param dependencies the dependencies set
     * @return the new visitor
     */
    public static ExpressionVisitor getDependenciesVisitor(
            HashSet<DbObject> dependencies) {
        return new ExpressionVisitor(GET_DEPENDENCIES, 0, dependencies, null,
                null, null, null, null);
    }

    /**
     * Create a new visitor to check if all aggregates are for the given table.
     *
     * @param table the table
     * @return the new visitor
     */
    public static ExpressionVisitor getOptimizableVisitor(Table table) {
        return new ExpressionVisitor(OPTIMIZABLE_AGGREGATE, 0, null,
                null, table, null, null, null);
    }

    /**
     * Create a new visitor to check if no expression depends on the given
     * resolver.
     *
     * @param resolver the resolver
     * @return the new visitor
     */
    public static ExpressionVisitor getNotFromResolverVisitor(ColumnResolver resolver) {
        return new ExpressionVisitor(NOT_FROM_RESOLVER, 0, null, null, null,
                resolver, null, null);
    }

    /**
     * Create a new visitor to get all referenced columns.
     *
     * @param columns the columns map
     * @return the new visitor
     */
    public static ExpressionVisitor getColumnsVisitor(AllColumnsForPlan columns) {
        return new ExpressionVisitor(GET_COLUMNS1, 0, null, columns, null, null, null, null);
    }

    /**
     * Create a new visitor to get all referenced columns.
     *
     * @param columns the columns map
     * @param table table to gather columns from, or {@code null} to gather all columns
     * @return the new visitor
     */
    public static ExpressionVisitor getColumnsVisitor(HashSet<Column> columns, Table table) {
        return new ExpressionVisitor(GET_COLUMNS2, 0, null, null, table, null, null, columns);
    }

    public static ExpressionVisitor getMaxModificationIdVisitor() {
        return new ExpressionVisitor(SET_MAX_DATA_MODIFICATION_ID, 0, null,
                null, null, null, new long[1], null);
    }

    /**
     * Add a new dependency to the set of dependencies.
     * This is used for GET_DEPENDENCIES visitors.
     *
     * @param obj the additional dependency.
     */
    public void addDependency(DbObject obj) {
        dependencies.add(obj);
    }

    /**
     * Add a new column to the set of columns.
     * This is used for GET_COLUMNS visitors.
     *
     * @param column the additional column.
     */
    void addColumn1(Column column) {
        columns1.add(column);
    }

    /**
     * Add a new column to the set of columns.
     * This is used for GET_COLUMNS2 visitors.
     *
     * @param column the additional column.
     */
    void addColumn2(Column column) {
        if (table == null || table == column.getTable()) {
            columns2.add(column);
        }
    }

    /**
     * Get the dependency set.
     * This is used for GET_DEPENDENCIES visitors.
     *
     * @return the set
     */
    public HashSet<DbObject> getDependencies() {
        return dependencies;
    }

    /**
     * Increment or decrement the query level.
     *
     * @param offset 1 to increment, -1 to decrement
     * @return this visitor or its clone with the changed query level
     */
    public ExpressionVisitor incrementQueryLevel(int offset) {
        if (type == INDEPENDENT) {
            offset += queryLevel;
            return offset < CACHED ? INDEPENDENT_VISITORS[offset] : new ExpressionVisitor(INDEPENDENT, offset);
        } else if (type == EVALUATABLE) {
            offset += queryLevel;
            return offset < CACHED ? EVALUATABLE_VISITORS[offset] : new ExpressionVisitor(EVALUATABLE, offset);
        } else {
            return this;
        }
    }

    /**
     * Get the column resolver.
     * This is used for NOT_FROM_RESOLVER visitors.
     *
     * @return the column resolver
     */
    public ColumnResolver getResolver() {
        return resolver;
    }

    /**
     * Update the field maxDataModificationId if this value is higher
     * than the current value.
     * This is used for SET_MAX_DATA_MODIFICATION_ID visitors.
     *
     * @param value the data modification id
     */
    public void addDataModificationId(long value) {
        long m = maxDataModificationId[0];
        if (value > m) {
            maxDataModificationId[0] = value;
        }
    }

    /**
     * Get the last data modification.
     * This is used for SET_MAX_DATA_MODIFICATION_ID visitors.
     *
     * @return the maximum modification id
     */
    public long getMaxDataModificationId() {
        return maxDataModificationId[0];
    }

    int getQueryLevel() {
        assert type == INDEPENDENT || type == EVALUATABLE;
        return queryLevel;
    }

    /**
     * Get the table.
     * This is used for OPTIMIZABLE_MIN_MAX_COUNT_ALL visitors.
     *
     * @return the table
     */
    public Table getTable() {
        return table;
    }

    /**
     * Get the visitor type.
     *
     * @return the type
     */
    public int getType() {
        return type;
    }

    /**
     * Get the set of columns of all tables.
     *
     * @param filters the filters
     * @param allColumnsSet the on-demand all-columns set
     */
    public static void allColumnsForTableFilters(TableFilter[] filters, AllColumnsForPlan allColumnsSet) {
        for (TableFilter filter : filters) {
            if (filter.getSelect() != null) {
                filter.getSelect().isEverything(ExpressionVisitor.getColumnsVisitor(allColumnsSet));
            }
        }
    }

}
