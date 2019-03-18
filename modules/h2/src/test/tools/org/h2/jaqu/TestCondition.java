/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jaqu;

import org.h2.jaqu.util.ClassUtils;

/**
 * This class represents an incomplete condition.
 *
 * @param <A> the incomplete condition data type
 */
public class TestCondition<A> {

    private final A x;

    public TestCondition(A x) {
        this.x = x;
    }

    public Boolean is(A y) {
        Boolean o = ClassUtils.newObject(Boolean.class);
        return Db.registerToken(o, new Function("=", x, y) {
            @Override
            public <T> void appendSQL(SQLStatement stat, Query<T> query) {
                stat.appendSQL("(");
                query.appendSQL(stat, x[0]);
                stat.appendSQL(" = ");
                query.appendSQL(stat, x[1]);
                stat.appendSQL(")");
            }
        });
    }

    public Boolean bigger(A y) {
        Boolean o = ClassUtils.newObject(Boolean.class);
        return Db.registerToken(o, new Function(">", x, y) {
            @Override
            public <T> void appendSQL(SQLStatement stat, Query<T> query) {
                stat.appendSQL("(");
                query.appendSQL(stat, x[0]);
                stat.appendSQL(" > ");
                query.appendSQL(stat, x[1]);
                stat.appendSQL(")");
            }
        });
    }

    public Boolean biggerEqual(A y) {
        Boolean o = ClassUtils.newObject(Boolean.class);
        return Db.registerToken(o, new Function(">=", x, y) {
            @Override
            public <T> void appendSQL(SQLStatement stat, Query<T> query) {
                stat.appendSQL("(");
                query.appendSQL(stat, x[0]);
                stat.appendSQL(" >= ");
                query.appendSQL(stat, x[1]);
                stat.appendSQL(")");
            }
        });
    }

    public Boolean smaller(A y) {
        Boolean o = ClassUtils.newObject(Boolean.class);
        return Db.registerToken(o, new Function("<", x, y) {
            @Override
            public <T> void appendSQL(SQLStatement stat, Query<T> query) {
                stat.appendSQL("(");
                query.appendSQL(stat, x[0]);
                stat.appendSQL(" < ");
                query.appendSQL(stat, x[1]);
                stat.appendSQL(")");
            }
        });
    }

    public Boolean smallerEqual(A y) {
        Boolean o = ClassUtils.newObject(Boolean.class);
        return Db.registerToken(o, new Function("<=", x, y) {
            @Override
            public <T> void appendSQL(SQLStatement stat, Query<T> query) {
                stat.appendSQL("(");
                query.appendSQL(stat, x[0]);
                stat.appendSQL(" <= ");
                query.appendSQL(stat, x[1]);
                stat.appendSQL(")");
            }
        });
    }

    public Boolean like(A pattern) {
        Boolean o = ClassUtils.newObject(Boolean.class);
        return Db.registerToken(o, new Function("LIKE", x, pattern) {
            @Override
            public <T> void appendSQL(SQLStatement stat, Query<T> query) {
                stat.appendSQL("(");
                query.appendSQL(stat, x[0]);
                stat.appendSQL(" LIKE ");
                query.appendSQL(stat, x[1]);
                stat.appendSQL(")");
            }
        });
    }

}
