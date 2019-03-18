/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jaqu;

import java.util.List;

/**
 * This class represents a query with a condition.
 *
 * @param <T> the return type
 */
public class QueryWhere<T> {

    Query<T> query;

    QueryWhere(Query<T> query) {
        this.query = query;
    }

    public <A> QueryCondition<T, A> and(A x) {
        query.addConditionToken(ConditionAndOr.AND);
        return new QueryCondition<>(query, x);
    }

    public <A> QueryCondition<T, A> or(A x) {
        query.addConditionToken(ConditionAndOr.OR);
        return new QueryCondition<>(query, x);
    }

    public QueryWhere<T> limit(long limit) {
        query.limit(limit);
        return this;
    }

    public QueryWhere<T> offset(long offset) {
        query.offset(offset);
        return this;
    }

    public <X, Z> List<X> select(Z x) {
        return query.select(x);
    }

    public String getSQL() {
        SQLStatement stat = new SQLStatement(query.getDb());
        stat.appendSQL("SELECT *");
        query.appendFromWhere(stat);
        return stat.getSQL().trim();
    }

    public <X, Z> List<X> selectDistinct(Z x) {
        return query.selectDistinct(x);
    }

    public <X, Z> X selectFirst(Z x) {
        List<X> list = query.select(x);
        return list.isEmpty() ? null : list.get(0);
    }

    public List<T> select() {
        return query.select();
    }

    public T selectFirst() {
        List<T> list = select();
        return list.isEmpty() ? null : list.get(0);
    }

    public List<T> selectDistinct() {
        return query.selectDistinct();
    }


    /**
     * Order by a number of columns.
     *
     * @param expressions the order by expressions
     * @return the query
     */
    public QueryWhere<T> orderBy(Object... expressions) {
        for (Object expr : expressions) {
            OrderExpression<T> e =
                new OrderExpression<>(query, expr, false, false, false);
            query.addOrderBy(e);
        }
        return this;
    }

    public QueryWhere<T> orderByNullsFirst(Object expr) {
        OrderExpression<T> e =
            new OrderExpression<>(query, expr, false, true, false);
        query.addOrderBy(e);
        return this;
    }

    public QueryWhere<T> orderByNullsLast(Object expr) {
        OrderExpression<T> e =
            new OrderExpression<>(query, expr, false, false, true);
        query.addOrderBy(e);
        return this;
    }

    public QueryWhere<T> orderByDesc(Object expr) {
        OrderExpression<T> e =
            new OrderExpression<>(query, expr, true, false, false);
        query.addOrderBy(e);
        return this;
    }

    public QueryWhere<T> orderByDescNullsFirst(Object expr) {
        OrderExpression<T> e =
            new OrderExpression<>(query, expr, true, true, false);
        query.addOrderBy(e);
        return this;
    }

    public QueryWhere<T> orderByDescNullsLast(Object expr) {
        OrderExpression<T> e =
            new OrderExpression<>(query, expr, true, false, true);
        query.addOrderBy(e);
        return this;
    }

    public int delete() {
        return query.delete();
    }

    public int update() {
        return query.update();
    }

    public long selectCount() {
        return query.selectCount();
    }

}
