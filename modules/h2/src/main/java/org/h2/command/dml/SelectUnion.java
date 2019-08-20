/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.util.ArrayList;
import java.util.HashSet;
import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.engine.Session;
import org.h2.engine.SysProperties;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.ExpressionVisitor;
import org.h2.expression.Parameter;
import org.h2.expression.ValueExpression;
import org.h2.message.DbException;
import org.h2.result.LazyResult;
import org.h2.result.LocalResult;
import org.h2.result.ResultInterface;
import org.h2.result.ResultTarget;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.ColumnResolver;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.util.ColumnNamer;
import org.h2.util.New;
import org.h2.util.StringUtils;
import org.h2.value.Value;
import org.h2.value.ValueInt;
import org.h2.value.ValueNull;

/**
 * Represents a union SELECT statement.
 */
public class SelectUnion extends Query {

    public enum UnionType {
        /**
         * The type of a UNION statement.
         */
        UNION,

        /**
         * The type of a UNION ALL statement.
         */
        UNION_ALL,

        /**
         * The type of an EXCEPT statement.
         */
        EXCEPT,

        /**
         * The type of an INTERSECT statement.
         */
        INTERSECT
    }

    private UnionType unionType;

    /**
     * The left hand side of the union (the first subquery).
     */
    final Query left;

    /**
     * The right hand side of the union (the second subquery).
     */
    Query right;

    private ArrayList<Expression> expressions;
    private Expression[] expressionArray;
    private ArrayList<SelectOrderBy> orderList;
    private SortOrder sort;
    private boolean isPrepared, checkInit;
    private boolean isForUpdate;

    public SelectUnion(Session session, Query query) {
        super(session);
        this.left = query;
    }

    @Override
    public boolean isUnion() {
        return true;
    }

    @Override
    public void prepareJoinBatch() {
        left.prepareJoinBatch();
        right.prepareJoinBatch();
    }

    public void setUnionType(UnionType type) {
        this.unionType = type;
    }

    public UnionType getUnionType() {
        return unionType;
    }

    public void setRight(Query select) {
        right = select;
    }

    public Query getLeft() {
        return left;
    }

    public Query getRight() {
        return right;
    }

    @Override
    public void setSQL(String sql) {
        this.sqlStatement = sql;
    }

    @Override
    public void setOrder(ArrayList<SelectOrderBy> order) {
        orderList = order;
    }

    @Override
    public boolean hasOrder() {
        return orderList != null || sort != null;
    }

    private Value[] convert(Value[] values, int columnCount) {
        Value[] newValues;
        if (columnCount == values.length) {
            // re-use the array if possible
            newValues = values;
        } else {
            // create a new array if needed,
            // for the value hash set
            newValues = new Value[columnCount];
        }
        for (int i = 0; i < columnCount; i++) {
            Expression e = expressions.get(i);
            newValues[i] = values[i].convertTo(e.getType());
        }
        return newValues;
    }

    @Override
    public ResultInterface queryMeta() {
        int columnCount = left.getColumnCount();
        LocalResult result = new LocalResult(session, expressionArray, columnCount);
        result.done();
        return result;
    }

    public LocalResult getEmptyResult() {
        int columnCount = left.getColumnCount();
        return new LocalResult(session, expressionArray, columnCount);
    }

    @Override
    protected ResultInterface queryWithoutCache(int maxRows, ResultTarget target) {
        if (maxRows != 0) {
            // maxRows is set (maxRows 0 means no limit)
            int l;
            if (limitExpr == null) {
                l = -1;
            } else {
                Value v = limitExpr.getValue(session);
                l = v == ValueNull.INSTANCE ? -1 : v.getInt();
            }
            if (l < 0) {
                // for limitExpr, 0 means no rows, and -1 means no limit
                l = maxRows;
            } else {
                l = Math.min(l, maxRows);
            }
            limitExpr = ValueExpression.get(ValueInt.get(l));
        }
        if (session.getDatabase().getSettings().optimizeInsertFromSelect) {
            if (unionType == UnionType.UNION_ALL && target != null) {
                if (sort == null && !distinct && maxRows == 0 &&
                        offsetExpr == null && limitExpr == null) {
                    left.query(0, target);
                    right.query(0, target);
                    return null;
                }
            }
        }
        int columnCount = left.getColumnCount();
        if (session.isLazyQueryExecution() && unionType == UnionType.UNION_ALL && !distinct &&
                sort == null && !randomAccessResult && !isForUpdate &&
                offsetExpr == null && isReadOnly()) {
            int limit = -1;
            if (limitExpr != null) {
                Value v = limitExpr.getValue(session);
                if (v != ValueNull.INSTANCE) {
                    limit = v.getInt();
                }
            }
            // limit 0 means no rows
            if (limit != 0) {
                LazyResultUnion lazyResult = new LazyResultUnion(expressionArray, columnCount);
                if (limit > 0) {
                    lazyResult.setLimit(limit);
                }
                return lazyResult;
            }
        }
        LocalResult result = new LocalResult(session, expressionArray, columnCount);
        if (sort != null) {
            result.setSortOrder(sort);
        }
        if (distinct) {
            left.setDistinct(true);
            right.setDistinct(true);
            result.setDistinct();
        }
        if (randomAccessResult) {
            result.setRandomAccess();
        }
        switch (unionType) {
        case UNION:
        case EXCEPT:
            left.setDistinct(true);
            right.setDistinct(true);
            result.setDistinct();
            break;
        case UNION_ALL:
            break;
        case INTERSECT:
            left.setDistinct(true);
            right.setDistinct(true);
            break;
        default:
            DbException.throwInternalError("type=" + unionType);
        }
        ResultInterface l = left.query(0);
        ResultInterface r = right.query(0);
        l.reset();
        r.reset();
        switch (unionType) {
        case UNION_ALL:
        case UNION: {
            while (l.next()) {
                result.addRow(convert(l.currentRow(), columnCount));
            }
            while (r.next()) {
                result.addRow(convert(r.currentRow(), columnCount));
            }
            break;
        }
        case EXCEPT: {
            while (l.next()) {
                result.addRow(convert(l.currentRow(), columnCount));
            }
            while (r.next()) {
                result.removeDistinct(convert(r.currentRow(), columnCount));
            }
            break;
        }
        case INTERSECT: {
            LocalResult temp = new LocalResult(session, expressionArray, columnCount);
            temp.setDistinct();
            temp.setRandomAccess();
            while (l.next()) {
                temp.addRow(convert(l.currentRow(), columnCount));
            }
            while (r.next()) {
                Value[] values = convert(r.currentRow(), columnCount);
                if (temp.containsDistinct(values)) {
                    result.addRow(values);
                }
            }
            temp.close();
            break;
        }
        default:
            DbException.throwInternalError("type=" + unionType);
        }
        if (offsetExpr != null) {
            result.setOffset(offsetExpr.getValue(session).getInt());
        }
        if (limitExpr != null) {
            Value v = limitExpr.getValue(session);
            if (v != ValueNull.INSTANCE) {
                result.setLimit(v.getInt());
            }
        }
        l.close();
        r.close();
        result.done();
        if (target != null) {
            while (result.next()) {
                target.addRow(result.currentRow());
            }
            result.close();
            return null;
        }
        return result;
    }

    @Override
    public void init() {
        if (SysProperties.CHECK && checkInit) {
            DbException.throwInternalError();
        }
        checkInit = true;
        left.init();
        right.init();
        int len = left.getColumnCount();
        if (len != right.getColumnCount()) {
            throw DbException.get(ErrorCode.COLUMN_COUNT_DOES_NOT_MATCH);
        }
        ArrayList<Expression> le = left.getExpressions();
        // set the expressions to get the right column count and names,
        // but can't validate at this time
        expressions = New.arrayList();
        for (int i = 0; i < len; i++) {
            Expression l = le.get(i);
            expressions.add(l);
        }
    }

    @Override
    public void prepare() {
        if (isPrepared) {
            // sometimes a subquery is prepared twice (CREATE TABLE AS SELECT)
            return;
        }
        if (SysProperties.CHECK && !checkInit) {
            DbException.throwInternalError("not initialized");
        }
        isPrepared = true;
        left.prepare();
        right.prepare();
        int len = left.getColumnCount();
        // set the correct expressions now
        expressions = New.arrayList();
        ArrayList<Expression> le = left.getExpressions();
        ArrayList<Expression> re = right.getExpressions();
        ColumnNamer columnNamer= new ColumnNamer(session);
        for (int i = 0; i < len; i++) {
            Expression l = le.get(i);
            Expression r = re.get(i);
            int type = Value.getHigherOrder(l.getType(), r.getType());
            long prec = Math.max(l.getPrecision(), r.getPrecision());
            int scale = Math.max(l.getScale(), r.getScale());
            int displaySize = Math.max(l.getDisplaySize(), r.getDisplaySize());
            String columnName = columnNamer.getColumnName(l,i,l.getAlias());
            Column col = new Column(columnName, type, prec, scale, displaySize);
            Expression e = new ExpressionColumn(session.getDatabase(), col);
            expressions.add(e);
        }
        if (orderList != null) {
            initOrder(session, expressions, null, orderList, getColumnCount(), true, null);
            sort = prepareOrder(orderList, expressions.size());
            orderList = null;
        }
        expressionArray = expressions.toArray(new Expression[0]);
    }

    @Override
    public double getCost() {
        return left.getCost() + right.getCost();
    }

    @Override
    public HashSet<Table> getTables() {
        HashSet<Table> set = left.getTables();
        set.addAll(right.getTables());
        return set;
    }

    @Override
    public ArrayList<Expression> getExpressions() {
        return expressions;
    }

    @Override
    public void setForUpdate(boolean forUpdate) {
        left.setForUpdate(forUpdate);
        right.setForUpdate(forUpdate);
        isForUpdate = forUpdate;
    }

    @Override
    public int getColumnCount() {
        return left.getColumnCount();
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level) {
        left.mapColumns(resolver, level);
        right.mapColumns(resolver, level);
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        left.setEvaluatable(tableFilter, b);
        right.setEvaluatable(tableFilter, b);
    }

    @Override
    public void addGlobalCondition(Parameter param, int columnId,
            int comparisonType) {
        addParameter(param);
        switch (unionType) {
        case UNION_ALL:
        case UNION:
        case INTERSECT: {
            left.addGlobalCondition(param, columnId, comparisonType);
            right.addGlobalCondition(param, columnId, comparisonType);
            break;
        }
        case EXCEPT: {
            left.addGlobalCondition(param, columnId, comparisonType);
            break;
        }
        default:
            DbException.throwInternalError("type=" + unionType);
        }
    }

    @Override
    public String getPlanSQL() {
        StringBuilder buff = new StringBuilder();
        buff.append('(').append(left.getPlanSQL()).append(')');
        switch (unionType) {
        case UNION_ALL:
            buff.append("\nUNION ALL\n");
            break;
        case UNION:
            buff.append("\nUNION\n");
            break;
        case INTERSECT:
            buff.append("\nINTERSECT\n");
            break;
        case EXCEPT:
            buff.append("\nEXCEPT\n");
            break;
        default:
            DbException.throwInternalError("type=" + unionType);
        }
        buff.append('(').append(right.getPlanSQL()).append(')');
        Expression[] exprList = expressions.toArray(new Expression[0]);
        if (sort != null) {
            buff.append("\nORDER BY ").append(sort.getSQL(exprList, exprList.length));
        }
        if (limitExpr != null) {
            buff.append("\nLIMIT ").append(
                    StringUtils.unEnclose(limitExpr.getSQL()));
            if (offsetExpr != null) {
                buff.append("\nOFFSET ").append(
                        StringUtils.unEnclose(offsetExpr.getSQL()));
            }
        }
        if (sampleSizeExpr != null) {
            buff.append("\nSAMPLE_SIZE ").append(
                    StringUtils.unEnclose(sampleSizeExpr.getSQL()));
        }
        if (isForUpdate) {
            buff.append("\nFOR UPDATE");
        }
        return buff.toString();
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        return left.isEverything(visitor) && right.isEverything(visitor);
    }

    @Override
    public boolean isReadOnly() {
        return left.isReadOnly() && right.isReadOnly();
    }

    @Override
    public void updateAggregate(Session s) {
        left.updateAggregate(s);
        right.updateAggregate(s);
    }

    @Override
    public void fireBeforeSelectTriggers() {
        left.fireBeforeSelectTriggers();
        right.fireBeforeSelectTriggers();
    }

    @Override
    public int getType() {
        return CommandInterface.SELECT;
    }

    @Override
    public boolean allowGlobalConditions() {
        return left.allowGlobalConditions() && right.allowGlobalConditions();
    }

    /**
     * Lazy execution for this union.
     */
    private final class LazyResultUnion extends LazyResult {

        int columnCount;
        ResultInterface l;
        ResultInterface r;
        boolean leftDone;
        boolean rightDone;

        LazyResultUnion(Expression[] expressions, int columnCount) {
            super(expressions);
            this.columnCount = columnCount;
        }

        @Override
        public int getVisibleColumnCount() {
            return columnCount;
        }

        @Override
        protected Value[] fetchNextRow() {
            if (rightDone) {
                return null;
            }
            if (!leftDone) {
                if (l == null) {
                    l = left.query(0);
                    l.reset();
                }
                if (l.next()) {
                    return l.currentRow();
                }
                leftDone = true;
            }
            if (r == null) {
                r = right.query(0);
                r.reset();
            }
            if (r.next()) {
                return r.currentRow();
            }
            rightDone = true;
            return null;
        }

        @Override
        public void close() {
            super.close();
            if (l != null) {
                l.close();
            }
            if (r != null) {
                r.close();
            }
        }

        @Override
        public void reset() {
            super.reset();
            if (l != null) {
                l.reset();
            }
            if (r != null) {
                r.reset();
            }
            leftDone = false;
            rightDone = false;
        }
    }
}
