/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jaqu;

/**
 * This class represents a query with join and an incomplete condition.
 *
 * @param <A> the incomplete condition data type
 */
public class QueryJoinCondition<A> {

    private final Query<?> query;
    private final SelectTable<?> join;
    private final A x;

    QueryJoinCondition(Query<?> query, SelectTable<?> join, A x) {
        this.query = query;
        this.join = join;
        this.x = x;
    }

    public Query<?> is(A y) {
        join.addConditionToken(new Condition<>(x, y, CompareType.EQUAL));
        return query;
    }
}
