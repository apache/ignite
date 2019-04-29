/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.command.Prepared;
import org.h2.engine.Database;
import org.h2.engine.Mode.ModeEnum;
import org.h2.engine.Session;
import org.h2.expression.Alias;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.ExpressionVisitor;
import org.h2.expression.Parameter;
import org.h2.expression.ValueExpression;
import org.h2.expression.function.FunctionCall;
import org.h2.message.DbException;
import org.h2.result.ResultInterface;
import org.h2.result.ResultTarget;
import org.h2.result.SortOrder;
import org.h2.table.ColumnResolver;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.util.StringUtils;
import org.h2.util.Utils;
import org.h2.value.Value;
import org.h2.value.ValueInt;
import org.h2.value.ValueNull;

/**
 * Represents a SELECT statement (simple, or union).
 */
public abstract class Query extends Prepared {

    /**
     * The column list, including invisible expressions such as order by expressions.
     */
    ArrayList<Expression> expressions;

    /**
     * Array of expressions.
     *
     * @see #expressions
     */
    Expression[] expressionArray;

    /**
     * Describes elements of the ORDER BY clause of a query.
     */
    ArrayList<SelectOrderBy> orderList;

    /**
     *  A sort order represents an ORDER BY clause in a query.
     */
    SortOrder sort;

    /**
     * The limit expression as specified in the LIMIT or TOP clause.
     */
    Expression limitExpr;

    /**
     * Whether limit expression specifies percentage of rows.
     */
    boolean fetchPercent;

    /**
     * Whether tied rows should be included in result too.
     */
    boolean withTies;

    /**
     * The offset expression as specified in the LIMIT .. OFFSET clause.
     */
    Expression offsetExpr;

    /**
     * The sample size expression as specified in the SAMPLE_SIZE clause.
     */
    Expression sampleSizeExpr;

    /**
     * Whether the result must only contain distinct rows.
     */
    boolean distinct;

    /**
     * Whether the result needs to support random access.
     */
    boolean randomAccessResult;

    private boolean noCache;
    private int lastLimit;
    private long lastEvaluated;
    private ResultInterface lastResult;
    private Value[] lastParameters;
    private boolean cacheableChecked;
    private boolean neverLazy;

    Query(Session session) {
        super(session);
    }

    public void setNeverLazy(boolean b) {
        this.neverLazy = b;
    }

    public boolean isNeverLazy() {
        return neverLazy;
    }

    /**
     * Check if this is a UNION query.
     *
     * @return {@code true} if this is a UNION query
     */
    public abstract boolean isUnion();

    /**
     * Prepare join batching.
     */
    public abstract void prepareJoinBatch();

    /**
     * Execute the query without checking the cache. If a target is specified,
     * the results are written to it, and the method returns null. If no target
     * is specified, a new LocalResult is created and returned.
     *
     * @param limit the limit as specified in the JDBC method call
     * @param target the target to write results to
     * @return the result
     */
    protected abstract ResultInterface queryWithoutCache(int limit,
            ResultTarget target);

    private ResultInterface queryWithoutCacheLazyCheck(int limit,
            ResultTarget target) {
        boolean disableLazy = neverLazy && session.isLazyQueryExecution();
        if (disableLazy) {
            session.setLazyQueryExecution(false);
        }
        try {
            return queryWithoutCache(limit, target);
        } finally {
            if (disableLazy) {
                session.setLazyQueryExecution(true);
            }
        }
    }

    /**
     * Initialize the query.
     */
    public abstract void init();

    /**
     * The the list of select expressions.
     * This may include invisible expressions such as order by expressions.
     *
     * @return the list of expressions
     */
    public ArrayList<Expression> getExpressions() {
        return expressions;
    }

    /**
     * Calculate the cost to execute this query.
     *
     * @return the cost
     */
    public abstract double getCost();

    /**
     * Calculate the cost when used as a subquery.
     * This method returns a value between 10 and 1000000,
     * to ensure adding other values can't result in an integer overflow.
     *
     * @return the estimated cost as an integer
     */
    public int getCostAsExpression() {
        // ensure the cost is not larger than 1 million,
        // so that adding other values can't overflow
        return (int) Math.min(1_000_000d, 10d + 10d * getCost());
    }

    /**
     * Get all tables that are involved in this query.
     *
     * @return the set of tables
     */
    public abstract HashSet<Table> getTables();

    /**
     * Set the order by list.
     *
     * @param order the order by list
     */
    public void setOrder(ArrayList<SelectOrderBy> order) {
        orderList = order;
    }

    /**
     * Whether the query has an order.
     *
     * @return true if it has
     */
    public boolean hasOrder() {
        return orderList != null || sort != null;
    }

    /**
     * Set the 'for update' flag.
     *
     * @param forUpdate the new setting
     */
    public abstract void setForUpdate(boolean forUpdate);

    /**
     * Get the column count of this query.
     *
     * @return the column count
     */
    public abstract int getColumnCount();

    /**
     * Map the columns to the given column resolver.
     *
     * @param resolver
     *            the resolver
     * @param level
     *            the subquery level (0 is the top level query, 1 is the first
     *            subquery level)
     */
    public abstract void mapColumns(ColumnResolver resolver, int level);

    /**
     * Change the evaluatable flag. This is used when building the execution
     * plan.
     *
     * @param tableFilter the table filter
     * @param b the new value
     */
    public abstract void setEvaluatable(TableFilter tableFilter, boolean b);

    /**
     * Add a condition to the query. This is used for views.
     *
     * @param param the parameter
     * @param columnId the column index (0 meaning the first column)
     * @param comparisonType the comparison type
     */
    public abstract void addGlobalCondition(Parameter param, int columnId,
            int comparisonType);

    /**
     * Check whether adding condition to the query is allowed. This is not
     * allowed for views that have an order by and a limit, as it would affect
     * the returned results.
     *
     * @return true if adding global conditions is allowed
     */
    public abstract boolean allowGlobalConditions();

    /**
     * Check if this expression and all sub-expressions can fulfill a criteria.
     * If any part returns false, the result is false.
     *
     * @param visitor the visitor
     * @return if the criteria can be fulfilled
     */
    public abstract boolean isEverything(ExpressionVisitor visitor);

    /**
     * Update all aggregate function values.
     *
     * @param s the session
     * @param stage select stage
     */
    public abstract void updateAggregate(Session s, int stage);

    /**
     * Call the before triggers on all tables.
     */
    public abstract void fireBeforeSelectTriggers();

    /**
     * Set the distinct flag.
     */
    public void setDistinct() {
        distinct = true;
    }

    /**
     * Set the distinct flag only if it is possible, may be used as a possible
     * optimization only.
     */
    public abstract void setDistinctIfPossible();

    /**
     * @return whether this query is a plain {@code DISTINCT} query
     */
    public boolean isStandardDistinct() {
        return distinct;
    }

    /**
     * @return whether this query is a {@code DISTINCT} or
     *         {@code DISTINCT ON (...)} query
     */
    public boolean isAnyDistinct() {
        return distinct;
    }

    /**
     * Returns whether results support random access.
     *
     * @return whether results support random access
     */
    public boolean isRandomAccessResult() {
        return randomAccessResult;
    }

    /**
     * Whether results need to support random access.
     *
     * @param b the new value
     */
    public void setRandomAccessResult(boolean b) {
        randomAccessResult = b;
    }

    @Override
    public boolean isQuery() {
        return true;
    }

    @Override
    public boolean isTransactional() {
        return true;
    }

    /**
     * Disable caching of result sets.
     */
    public void disableCache() {
        this.noCache = true;
    }

    private boolean sameResultAsLast(Session s, Value[] params,
            Value[] lastParams, long lastEval) {
        if (!cacheableChecked) {
            long max = getMaxDataModificationId();
            noCache = max == Long.MAX_VALUE;
            if (!isEverything(ExpressionVisitor.DETERMINISTIC_VISITOR) ||
                    !isEverything(ExpressionVisitor.INDEPENDENT_VISITOR)) {
                noCache = true;
            }
            cacheableChecked = true;
        }
        if (noCache) {
            return false;
        }
        Database db = s.getDatabase();
        for (int i = 0; i < params.length; i++) {
            Value a = lastParams[i], b = params[i];
            if (a.getValueType() != b.getValueType() || !db.areEqual(a, b)) {
                return false;
            }
        }
        return getMaxDataModificationId() <= lastEval;
    }

    private  Value[] getParameterValues() {
        ArrayList<Parameter> list = getParameters();
        if (list == null) {
            return new Value[0];
        }
        int size = list.size();
        Value[] params = new Value[size];
        for (int i = 0; i < size; i++) {
            Value v = list.get(i).getParamValue();
            params[i] = v;
        }
        return params;
    }

    @Override
    public final ResultInterface query(int maxrows) {
        return query(maxrows, null);
    }

    /**
     * Execute the query, writing the result to the target result.
     *
     * @param limit the maximum number of rows to return
     * @param target the target result (null will return the result)
     * @return the result set (if the target is not set).
     */
    public final ResultInterface query(int limit, ResultTarget target) {
        if (isUnion()) {
            // union doesn't always know the parameter list of the left and
            // right queries
            return queryWithoutCacheLazyCheck(limit, target);
        }
        fireBeforeSelectTriggers();
        if (noCache || !session.getDatabase().getOptimizeReuseResults() ||
                (session.isLazyQueryExecution() && !neverLazy)) {
            return queryWithoutCacheLazyCheck(limit, target);
        }
        Value[] params = getParameterValues();
        long now = session.getDatabase().getModificationDataId();
        if (isEverything(ExpressionVisitor.DETERMINISTIC_VISITOR)) {
            if (lastResult != null && !lastResult.isClosed() &&
                    limit == lastLimit) {
                if (sameResultAsLast(session, params, lastParameters,
                        lastEvaluated)) {
                    lastResult = lastResult.createShallowCopy(session);
                    if (lastResult != null) {
                        lastResult.reset();
                        return lastResult;
                    }
                }
            }
        }
        lastParameters = params;
        closeLastResult();
        ResultInterface r = queryWithoutCacheLazyCheck(limit, target);
        lastResult = r;
        this.lastEvaluated = now;
        lastLimit = limit;
        return r;
    }

    private void closeLastResult() {
        if (lastResult != null) {
            lastResult.close();
        }
    }

    /**
     * Initialize the order by list. This call may extend the expressions list.
     *
     * @param session the session
     * @param expressions the select list expressions
     * @param expressionSQL the select list SQL snippets
     * @param orderList the order by list
     * @param visible the number of visible columns in the select list
     * @param mustBeInResult all order by expressions must be in the select list
     * @param filters the table filters
     */
    static void initOrder(Session session,
            ArrayList<Expression> expressions,
            ArrayList<String> expressionSQL,
            List<SelectOrderBy> orderList,
            int visible,
            boolean mustBeInResult,
            ArrayList<TableFilter> filters) {
        for (SelectOrderBy o : orderList) {
            Expression e = o.expression;
            if (e == null) {
                continue;
            }
            int idx = initExpression(session, expressions, expressionSQL, e, visible, mustBeInResult, filters);
            o.columnIndexExpr = ValueExpression.get(ValueInt.get(idx + 1));
            o.expression = expressions.get(idx).getNonAliasExpression();
        }
    }

    /**
     * Initialize the 'ORDER BY' or 'DISTINCT' expressions.
     *
     * @param session the session
     * @param expressions the select list expressions
     * @param expressionSQL the select list SQL snippets
     * @param e the expression.
     * @param visible the number of visible columns in the select list
     * @param mustBeInResult all order by expressions must be in the select list
     * @param filters the table filters.
     * @return index on the expression in the {@link #expressions} list.
     */
    static int initExpression(Session session, ArrayList<Expression> expressions,
            ArrayList<String> expressionSQL, Expression e, int visible, boolean mustBeInResult,
            ArrayList<TableFilter> filters) {
        Database db = session.getDatabase();
        // special case: SELECT 1 AS A FROM DUAL ORDER BY A
        // (oracle supports it, but only in order by, not in group by and
        // not in having):
        // SELECT 1 AS A FROM DUAL ORDER BY -A
        if (e instanceof ExpressionColumn) {
            // order by expression
            ExpressionColumn exprCol = (ExpressionColumn) e;
            String tableAlias = exprCol.getOriginalTableAliasName();
            String col = exprCol.getOriginalColumnName();
            for (int j = 0; j < visible; j++) {
                Expression ec = expressions.get(j);
                if (ec instanceof ExpressionColumn) {
                    // select expression
                    ExpressionColumn c = (ExpressionColumn) ec;
                    if (!db.equalsIdentifiers(col, c.getColumnName())) {
                        continue;
                    }
                    if (tableAlias == null) {
                        return j;
                    }
                    String ca = c.getOriginalTableAliasName();
                    if (ca != null) {
                        if (db.equalsIdentifiers(ca, tableAlias)) {
                            return j;
                        }
                    } else if (filters != null) {
                        // select id from test order by test.id
                        for (TableFilter f : filters) {
                            if (db.equalsIdentifiers(f.getTableAlias(), tableAlias)) {
                                return j;
                            }
                        }
                    }
                } else if (ec instanceof Alias) {
                    if (tableAlias == null && db.equalsIdentifiers(col, ec.getAlias())) {
                        return j;
                    }
                    Expression ec2 = ec.getNonAliasExpression();
                    if (ec2 instanceof ExpressionColumn) {
                        ExpressionColumn c2 = (ExpressionColumn) ec2;
                        String ta = exprCol.getSQL(true);
                        String tb = c2.getSQL(true);
                        String s2 = c2.getColumnName();
                        if (db.equalsIdentifiers(col, s2) && db.equalsIdentifiers(ta, tb)) {
                            return j;
                        }
                    }
                }
            }
        } else if (expressionSQL != null) {
            String s = e.getSQL(true);
            for (int j = 0, size = expressionSQL.size(); j < size; j++) {
                if (db.equalsIdentifiers(expressionSQL.get(j), s)) {
                    return j;
                }
            }
        }
        if (expressionSQL == null
                || mustBeInResult && session.getDatabase().getMode().getEnum() != ModeEnum.MySQL
                        && !checkOrderOther(session, e, expressionSQL)) {
            throw DbException.get(ErrorCode.ORDER_BY_NOT_IN_RESULT, e.getSQL(false));
        }
        int idx = expressions.size();
        expressions.add(e);
        expressionSQL.add(e.getSQL(true));
        return idx;
    }

    /**
     * An additional check for expression in ORDER BY list for DISTINCT selects
     * that was not matched with selected expressions in regular way. This
     * method allows expressions based only on selected expressions in different
     * complicated ways with functions, comparisons, or operators.
     *
     * @param session session
     * @param expr expression to check
     * @param expressionSQL SQL of allowed expressions
     * @return whether the specified expression should be allowed in ORDER BY
     *         list of DISTINCT select
     */
    private static boolean checkOrderOther(Session session, Expression expr, ArrayList<String> expressionSQL) {
        if (expr.isConstant()) {
            // ValueExpression or other
            return true;
        }
        String exprSQL = expr.getSQL(true);
        for (String sql: expressionSQL) {
            if (session.getDatabase().equalsIdentifiers(exprSQL, sql)) {
                return true;
            }
        }
        int count = expr.getSubexpressionCount();
        if (expr instanceof FunctionCall) {
            if (!((FunctionCall) expr).isDeterministic()) {
                return false;
            }
        } else if (count <= 0) {
            // Expression is an ExpressionColumn, Parameter, SequenceValue or
            // has other unsupported type without subexpressions
            return false;
        }
        for (int i = 0; i < count; i++) {
            if (!checkOrderOther(session, expr.getSubexpression(i), expressionSQL)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Create a {@link SortOrder} object given the list of {@link SelectOrderBy}
     * objects.
     *
     * @param orderList a list of {@link SelectOrderBy} elements
     * @param expressionCount the number of columns in the query
     * @return the {@link SortOrder} object
     */
    public SortOrder prepareOrder(ArrayList<SelectOrderBy> orderList, int expressionCount) {
        int size = orderList.size();
        int[] index = new int[size];
        int[] sortType = new int[size];
        for (int i = 0; i < size; i++) {
            SelectOrderBy o = orderList.get(i);
            int idx;
            boolean reverse = false;
            Value v = o.columnIndexExpr.getValue(null);
            if (v == ValueNull.INSTANCE) {
                // parameter not yet set - order by first column
                idx = 0;
            } else {
                idx = v.getInt();
                if (idx < 0) {
                    reverse = true;
                    idx = -idx;
                }
                idx -= 1;
                if (idx < 0 || idx >= expressionCount) {
                    throw DbException.get(ErrorCode.ORDER_BY_NOT_IN_RESULT, Integer.toString(idx + 1));
                }
            }
            index[i] = idx;
            int type = o.sortType;
            if (reverse) {
                // TODO NULLS FIRST / LAST should be inverted too?
                type ^= SortOrder.DESCENDING;
            }
            sortType[i] = type;
        }
        return new SortOrder(session.getDatabase(), index, sortType, orderList);
    }

    @Override
    public int getType() {
        return CommandInterface.SELECT;
    }

    public void setOffset(Expression offset) {
        this.offsetExpr = offset;
    }

    public Expression getOffset() {
        return offsetExpr;
    }

    public void setLimit(Expression limit) {
        this.limitExpr = limit;
    }

    public Expression getLimit() {
        return limitExpr;
    }

    public void setFetchPercent(boolean fetchPercent) {
        this.fetchPercent = fetchPercent;
    }

    public boolean isFetchPercent() {
        return fetchPercent;
    }

    public void setWithTies(boolean withTies) {
        this.withTies = withTies;
    }

    public boolean isWithTies() {
        return withTies;
    }

    /**
     * Add a parameter to the parameter list.
     *
     * @param param the parameter to add
     */
    void addParameter(Parameter param) {
        if (parameters == null) {
            parameters = Utils.newSmallArrayList();
        }
        parameters.add(param);
    }

    public void setSampleSize(Expression sampleSize) {
        this.sampleSizeExpr = sampleSize;
    }

    /**
     * Get the sample size, if set.
     *
     * @param session the session
     * @return the sample size
     */
    int getSampleSizeValue(Session session) {
        if (sampleSizeExpr == null) {
            return 0;
        }
        Value v = sampleSizeExpr.optimize(session).getValue(session);
        if (v == ValueNull.INSTANCE) {
            return 0;
        }
        return v.getInt();
    }

    public final long getMaxDataModificationId() {
        ExpressionVisitor visitor = ExpressionVisitor.getMaxModificationIdVisitor();
        isEverything(visitor);
        return visitor.getMaxDataModificationId();
    }

    /**
     * Appends query limits info to the plan.
     *
     * @param builder query plan string builder.
     * @param alwaysQuote quote all identifiers
     */
    void appendLimitToSQL(StringBuilder builder, boolean alwaysQuote) {
        if (offsetExpr != null) {
            String count = StringUtils.unEnclose(offsetExpr.getSQL(alwaysQuote));
            builder.append("\nOFFSET ").append(count).append("1".equals(count) ? " ROW" : " ROWS");
        }
        if (limitExpr != null) {
            builder.append("\nFETCH ").append(offsetExpr != null ? "NEXT" : "FIRST");
            String count = StringUtils.unEnclose(limitExpr.getSQL(alwaysQuote));
            boolean withCount = fetchPercent || !"1".equals(count);
            if (withCount) {
                builder.append(' ').append(count);
                if (fetchPercent) {
                    builder.append(" PERCENT");
                }
            }
            builder.append(!withCount ? " ROW" : " ROWS")
                    .append(withTies ? " WITH TIES" : " ONLY");
        }
    }

}
