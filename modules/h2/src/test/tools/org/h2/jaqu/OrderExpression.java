/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jaqu;

/**
 * An expression to order by in a query.
 *
 * @param <T> the query data type
 */
class OrderExpression<T> {
    private final Query<T> query;
    private final Object expression;
    private final boolean desc;
    private final boolean nullsFirst;
    private final boolean nullsLast;

    OrderExpression(Query<T> query, Object expression, boolean desc,
            boolean nullsFirst, boolean nullsLast) {
        this.query = query;
        this.expression = expression;
        this.desc = desc;
        this.nullsFirst = nullsFirst;
        this.nullsLast = nullsLast;
    }

    void appendSQL(SQLStatement stat) {
        query.appendSQL(stat, expression);
        if (desc) {
            stat.appendSQL(" DESC");
        }
        if (nullsLast) {
            stat.appendSQL(" NULLS LAST");
        }
        if (nullsFirst) {
            stat.appendSQL(" NULLS FIRST");
        }
    }

}
