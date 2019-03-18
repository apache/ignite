/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jaqu;

/**
 * This class represents a query with a join.
 */
public class QueryJoin {

    private final Query<?> query;
    private final SelectTable<?> join;

    QueryJoin(Query<?> query, SelectTable<?> join) {
        this.query = query;
        this.join = join;
    }

    public <A> QueryJoinCondition<A> on(A x) {
        return new QueryJoinCondition<>(query, join, x);
    }
}
