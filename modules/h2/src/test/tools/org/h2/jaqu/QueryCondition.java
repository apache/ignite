/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jaqu;

/**
 * This class represents a query with an incomplete condition.
 *
 * @param <T> the return type of the query
 * @param <A> the incomplete condition data type
 */
public class QueryCondition<T, A> {

    private final Query<T> query;
    private final A x;

    QueryCondition(Query<T> query, A x) {
        this.query = query;
        this.x = x;
    }

    public QueryWhere<T> is(A y) {
        query.addConditionToken(
                new Condition<>(x, y, CompareType.EQUAL));
        return new QueryWhere<>(query);
    }

    public QueryWhere<T> bigger(A y) {
        query.addConditionToken(
                new Condition<>(x, y, CompareType.BIGGER));
        return new QueryWhere<>(query);
    }

    public QueryWhere<T> biggerEqual(A y) {
        query.addConditionToken(
                new Condition<>(x, y, CompareType.BIGGER_EQUAL));
        return new QueryWhere<>(query);
    }

    public QueryWhere<T> smaller(A y) {
        query.addConditionToken(
                new Condition<>(x, y, CompareType.SMALLER));
        return new QueryWhere<>(query);
    }

    public QueryWhere<T> smallerEqual(A y) {
        query.addConditionToken(
                new Condition<>(x, y, CompareType.SMALLER_EQUAL));
        return new QueryWhere<>(query);
    }

    public QueryWhere<T> like(A pattern) {
        query.addConditionToken(
                new Condition<>(x, pattern, CompareType.LIKE));
        return new QueryWhere<>(query);
    }

}
