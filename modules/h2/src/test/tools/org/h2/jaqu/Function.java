/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jaqu;

import org.h2.jaqu.util.ClassUtils;

/**
 * This class provides static methods that represents common SQL functions.
 */
public class Function implements Token {

    // must be a new instance
    private static final Long COUNT_STAR = Long.valueOf(0);

    protected Object[] x;
    private final String name;

    protected Function(String name, Object... x) {
        this.name = name;
        this.x = x;
    }

    @Override
    public <T> void appendSQL(SQLStatement stat, Query<T> query) {
        stat.appendSQL(name).appendSQL("(");
        int i = 0;
        for (Object o : x) {
            if (i++ > 0) {
                stat.appendSQL(",");
            }
            query.appendSQL(stat, o);
        }
        stat.appendSQL(")");
    }

    public static Long count() {
        return COUNT_STAR;
    }

    public static Integer length(Object x) {
        return Db.registerToken(
            ClassUtils.newObject(Integer.class), new Function("LENGTH", x));
    }

    @SuppressWarnings("unchecked")
    public static <T extends Number> T sum(T x) {
        return (T) Db.registerToken(
            ClassUtils.newObject(x.getClass()), new Function("SUM", x));
    }

    public static Long count(Object x) {
        return Db.registerToken(
            ClassUtils.newObject(Long.class), new Function("COUNT", x));
    }

    public static Boolean isNull(Object x) {
        return Db.registerToken(
            ClassUtils.newObject(Boolean.class), new Function("", x) {
                @Override
                public <T> void appendSQL(SQLStatement stat, Query<T> query) {
                    query.appendSQL(stat, x[0]);
                    stat.appendSQL(" IS NULL");
                }
            });
    }

    public static Boolean isNotNull(Object x) {
        return Db.registerToken(
            ClassUtils.newObject(Boolean.class), new Function("", x) {
                @Override
                public <T> void appendSQL(SQLStatement stat, Query<T> query) {
                    query.appendSQL(stat, x[0]);
                    stat.appendSQL(" IS NOT NULL");
                }
            });
    }

    public static Boolean not(Boolean x) {
        return Db.registerToken(
            ClassUtils.newObject(Boolean.class), new Function("", x) {
                @Override
                public <T> void appendSQL(SQLStatement stat, Query<T> query) {
                    stat.appendSQL("NOT ");
                    query.appendSQL(stat, x[0]);
                }
            });
    }

    public static Boolean or(Boolean... x) {
        return Db.registerToken(
                ClassUtils.newObject(Boolean.class),
                new Function("", (Object[]) x) {
            @Override
            public <T> void appendSQL(SQLStatement stat, Query<T> query) {
                int i = 0;
                for (Object o : x) {
                    if (i++ > 0) {
                        stat.appendSQL(" OR ");
                    }
                    query.appendSQL(stat, o);
                }
            }
        });
    }

    public static Boolean and(Boolean... x) {
        return Db.registerToken(
                ClassUtils.newObject(Boolean.class),
                new Function("", (Object[]) x) {
            @Override
            public <T> void appendSQL(SQLStatement stat, Query<T> query) {
                int i = 0;
                for (Object o : x) {
                    if (i++ > 0) {
                        stat.appendSQL(" AND ");
                    }
                    query.appendSQL(stat, o);
                }
            }
        });
    }

    @SuppressWarnings("unchecked")
    public static <X> X min(X x) {
        Class<X> clazz = (Class<X>) x.getClass();
        X o = ClassUtils.newObject(clazz);
        return Db.registerToken(o, new Function("MIN", x));
    }

    @SuppressWarnings("unchecked")
    public static <X> X max(X x) {
        Class<X> clazz = (Class<X>) x.getClass();
        X o = ClassUtils.newObject(clazz);
        return Db.registerToken(o, new Function("MAX", x));
    }

    public static Boolean like(String x, String pattern) {
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
