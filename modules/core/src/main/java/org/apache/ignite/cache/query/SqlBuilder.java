package org.apache.ignite.cache.query;

import java.util.ArrayList;
import java.util.Collection;

public class SqlBuilder<K, V> {

    private static final String AND = " AND ";
    private StringBuilder sqlBuilder = new StringBuilder("1 = 1");
    private Collection<Object> sqlParams = new ArrayList<>();

    private void append(String name, String value, String operation) {
        if (value != null) {
            sqlBuilder.append(AND);
            sqlBuilder.append(name);
            sqlBuilder.append(operation);
            sqlParams.add(value);
        }
    }

    public static <K, V> SqlBuilder<K, V> create() {
        return new SqlBuilder<>();
    }

    public SqlBuilder<K, V> eq(String name, Object value) {
        return eq(name, value.toString());
    }

    public SqlBuilder<K, V> eq(String name, String value) {
        append(name, value, " = ?");
        return this;
    }

    public SqlBuilder<K, V> notEq(String name, Object value) {
        return notEq(name, value.toString());
    }

    public SqlBuilder<K, V> notEq(String name, String value) {
        append(name, value, " != ?");
        return this;
    }

    public SqlBuilder<K, V> more(String name, Object value) {
        return more(name, value.toString());
    }

    public SqlBuilder<K, V> more(String name, String value) {
        append(name, value, " > ?");
        return this;
    }

    public SqlBuilder less(String name, Object value) {
        return less(name, value.toString());
    }

    public SqlBuilder<K, V> less(String name, String value) {
        append(name, value, " < ?");
        return this;
    }

    public SqlQuery<K, V> build(Class<?> type) {
        SqlQuery<K, V> sqlQuery = new SqlQuery<>(type, sqlBuilder.toString());
        sqlQuery.setArgs(sqlParams.toArray());
        return sqlQuery;
    }
}