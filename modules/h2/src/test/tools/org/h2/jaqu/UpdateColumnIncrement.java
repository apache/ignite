/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: James Moger
 */
package org.h2.jaqu;

/**
 * This class represents "SET column = (column + x)" in an UPDATE statement.
 *
 * @param <T> the query type
 * @param <A> the new value data type
 */
public class UpdateColumnIncrement<T, A> implements UpdateColumn {

    private final Query<T> query;
    private final A x;
    private A y;

    UpdateColumnIncrement(Query<T> query, A x) {
        this.query = query;
        this.x = x;
    }

    public Query<T> by(A y) {
        query.addUpdateColumnDeclaration(this);
        this.y = y;
        return query;
    }

    @Override
    public void appendSQL(SQLStatement stat) {
        query.appendSQL(stat, x);
        stat.appendSQL("=(");
        query.appendSQL(stat, x);
        stat.appendSQL("+");
        query.appendSQL(stat, y);
        stat.appendSQL(")");
    }

}
