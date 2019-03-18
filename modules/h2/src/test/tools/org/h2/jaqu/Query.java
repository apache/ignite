/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jaqu;

import java.lang.reflect.Field;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import org.h2.jaqu.bytecode.ClassReader;
import org.h2.jaqu.util.StatementLogger;
import org.h2.jaqu.util.ClassUtils;
import org.h2.util.JdbcUtils;
import org.h2.util.New;

/**
 * This class represents a query.
 *
 * @param <T> the return type
 */
public class Query<T> {

    private final Db db;
    private SelectTable<T> from;
    private final ArrayList<Token> conditions = New.arrayList();
    private final ArrayList<UpdateColumn> updateColumnDeclarations = New
            .arrayList();
    private final ArrayList<SelectTable<?>> joins = New.arrayList();
    private final IdentityHashMap<Object, SelectColumn<T>> aliasMap = ClassUtils
            .newIdentityHashMap();
    private final ArrayList<OrderExpression<T>> orderByList = New.arrayList();
    private Object[] groupByExpressions;
    private long limit;
    private long offset;

    Query(Db db) {
        this.db = db;
    }

    @SuppressWarnings("unchecked")
    static <T> Query<T> from(Db db, T alias) {
        Query<T> query = new Query<>(db);
        TableDefinition<T> def = (TableDefinition<T>) db.define(alias
                .getClass());
        query.from = new SelectTable<>(db, query, alias, false);
        def.initSelectObject(query.from, alias, query.aliasMap);
        return query;
    }

    public long selectCount() {
        SQLStatement stat = getSelectStatement(false);
        stat.appendSQL("COUNT(*) ");
        appendFromWhere(stat);
        ResultSet rs = stat.executeQuery();
        Statement s = null;
        try {
            s = rs.getStatement();
            rs.next();
            long value = rs.getLong(1);
            return value;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            JdbcUtils.closeSilently(rs);
            JdbcUtils.closeSilently(s);
        }
    }

    public List<T> select() {
        return select(false);
    }

    public T selectFirst() {
        return select(false).get(0);
    }

    public List<T> selectDistinct() {
        return select(true);
    }

    @SuppressWarnings("unchecked")
    public <X, Z> X selectFirst(Z x) {
        List<X> list = (List<X>) select(x);
        return list.isEmpty() ? null : list.get(0);
    }

    public String getSQL() {
        SQLStatement stat = getSelectStatement(false);
        stat.appendSQL("*");
        appendFromWhere(stat);
        return stat.getSQL().trim();
    }

    private List<T> select(boolean distinct) {
        List<T> result = New.arrayList();
        TableDefinition<T> def = from.getAliasDefinition();
        SQLStatement stat = getSelectStatement(distinct);
        def.appendSelectList(stat);
        appendFromWhere(stat);
        ResultSet rs = stat.executeQuery();
        Statement s = null;
        try {
            s = rs.getStatement();
            while (rs.next()) {
                T item = from.newObject();
                from.getAliasDefinition().readRow(item, rs);
                result.add(item);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            JdbcUtils.closeSilently(rs);
            JdbcUtils.closeSilently(s);
        }
        return result;
    }

    public int delete() {
        SQLStatement stat = new SQLStatement(db);
        stat.appendSQL("DELETE FROM ");
        from.appendSQL(stat);
        appendWhere(stat);
        StatementLogger.delete(stat.getSQL());
        return stat.executeUpdate();
    }

    public <A> UpdateColumnSet<T, A> set(A field) {
        return new UpdateColumnSet<>(this, field);
    }

    public <A> UpdateColumnIncrement<T, A> increment(A field) {
        return new UpdateColumnIncrement<>(this, field);
    }

    public int update() {
        if (updateColumnDeclarations.size() == 0) {
            throw new RuntimeException("Missing set or increment call.");
        }
        SQLStatement stat = new SQLStatement(db);
        stat.appendSQL("UPDATE ");
        from.appendSQL(stat);
        stat.appendSQL(" SET ");
        int i = 0;
        for (UpdateColumn declaration : updateColumnDeclarations) {
            if (i++ > 0) {
                stat.appendSQL(", ");
            }
            declaration.appendSQL(stat);
        }
        appendWhere(stat);
        StatementLogger.update(stat.getSQL());
        return stat.executeUpdate();
    }

    public <X, Z> List<X> selectDistinct(Z x) {
        return select(x, true);
    }

    public <X, Z> List<X> select(Z x) {
        return select(x, false);
    }

    @SuppressWarnings("unchecked")
    private <X, Z> List<X> select(Z x, boolean distinct) {
        Class<?> clazz = x.getClass();
        if (ClassUtils.isSimpleType(clazz)) {
            return selectSimple((X) x, distinct);
        }
        clazz = clazz.getSuperclass();
        return select((Class<X>) clazz, (X) x, distinct);
    }

    private <X> List<X> select(Class<X> clazz, X x, boolean distinct) {
        List<X> result = New.arrayList();
        TableDefinition<X> def = db.define(clazz);
        SQLStatement stat = getSelectStatement(distinct);
        def.appendSelectList(stat, this, x);
        appendFromWhere(stat);
        ResultSet rs = stat.executeQuery();
        Statement s = null;
        try {
            s = rs.getStatement();
            while (rs.next()) {
                X row = ClassUtils.newObject(clazz);
                def.readRow(row, rs);
                result.add(row);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            JdbcUtils.closeSilently(rs);
            JdbcUtils.closeSilently(s);
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private <X> List<X> selectSimple(X x, boolean distinct) {
        SQLStatement stat = getSelectStatement(distinct);
        appendSQL(stat, x);
        appendFromWhere(stat);
        ResultSet rs = stat.executeQuery();
        List<X> result = New.arrayList();
        Statement s = null;
        try {
            s = rs.getStatement();
            while (rs.next()) {
                try {
                    X value;
                    Object o = rs.getObject(1);
                    int convertHereIsProbablyWrong;
                    if (Clob.class.isAssignableFrom(o.getClass())) {
                        value = (X) ClassUtils.convert(o, String.class);
                    } else {
                        value = (X) o;
                    }
                    result.add(value);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            JdbcUtils.closeSilently(rs);
            JdbcUtils.closeSilently(s);
        }
        return result;
    }

    private SQLStatement getSelectStatement(boolean distinct) {
        SQLStatement stat = new SQLStatement(db);
        stat.appendSQL("SELECT ");
        if (distinct) {
            stat.appendSQL("DISTINCT ");
        }
        return stat;
    }

    public <A> QueryCondition<T, A> where(A x) {
        return new QueryCondition<>(this, x);
    }

    public <A> QueryWhere<T> where(Filter filter) {
        HashMap<String, Object> fieldMap = new HashMap<>();
        for (Field f : filter.getClass().getDeclaredFields()) {
            f.setAccessible(true);
            try {
                Object obj = f.get(filter);
                if (obj == from.getAlias()) {
                    List<TableDefinition.FieldDefinition> fields = from
                            .getAliasDefinition().getFields();
                    String name = f.getName();
                    for (TableDefinition.FieldDefinition field : fields) {
                        String n = name + "." + field.field.getName();
                        Object o = field.field.get(obj);
                        fieldMap.put(n, o);
                    }
                }
                fieldMap.put(f.getName(), f.get(filter));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        Token filterCode = new ClassReader().decompile(filter, fieldMap,
                "where");
        // String filterQuery = filterCode.toString();
        conditions.add(filterCode);
        return new QueryWhere<>(this);
    }

    public QueryWhere<T> whereTrue(Boolean condition) {
        Token token = new Function("", condition);
        addConditionToken(token);
        return new QueryWhere<>(this);
    }

    /**
     * Sets the Limit and Offset of a query.
     *
     * @return the query
     */
    public Query<T> limit(long limit) {
        this.limit = limit;
        return this;
    }

    public Query<T> offset(long offset) {
        this.offset = offset;
        return this;
    }

    /**
     * Order by a number of columns.
     *
     * @param expressions the columns
     * @return the query
     */
    public Query<T> orderBy(Object... expressions) {
        for (Object expr : expressions) {
            OrderExpression<T> e = new OrderExpression<>(this, expr, false,
                    false, false);
            addOrderBy(e);
        }
        return this;
    }

    public Query<T> orderByDesc(Object expr) {
        OrderExpression<T> e = new OrderExpression<>(this, expr, true, false,
                false);
        addOrderBy(e);
        return this;
    }

    public Query<T> groupBy(Object... groupBy) {
        this.groupByExpressions = groupBy;
        return this;
    }

    /**
     * INTERNAL
     *
     * @param stat the statement
     * @param x the alias object
     */
    public void appendSQL(SQLStatement stat, Object x) {
        if (x == Function.count()) {
            stat.appendSQL("COUNT(*)");
            return;
        }
        Token token = Db.getToken(x);
        if (token != null) {
            token.appendSQL(stat, this);
            return;
        }
        SelectColumn<T> col = aliasMap.get(x);
        if (col != null) {
            col.appendSQL(stat);
            return;
        }
        stat.appendSQL("?");
        stat.addParameter(x);
    }

    void addConditionToken(Token condition) {
        conditions.add(condition);
    }

    void addUpdateColumnDeclaration(UpdateColumn declaration) {
        updateColumnDeclarations.add(declaration);
    }

    void appendWhere(SQLStatement stat) {
        if (!conditions.isEmpty()) {
            stat.appendSQL(" WHERE ");
            for (Token token : conditions) {
                token.appendSQL(stat, this);
                stat.appendSQL(" ");
            }
        }
    }

    @SuppressWarnings("unchecked")
    void appendFromWhere(SQLStatement stat) {
        stat.appendSQL(" FROM ");
        from.appendSQL(stat);
        for (SelectTable join : joins) {
            join.appendSQLAsJoin(stat, this);
        }
        appendWhere(stat);
        if (groupByExpressions != null) {
            stat.appendSQL(" GROUP BY ");
            int i = 0;
            for (Object obj : groupByExpressions) {
                if (i++ > 0) {
                    stat.appendSQL(", ");
                }
                appendSQL(stat, obj);
                stat.appendSQL(" ");
            }
        }
        if (!orderByList.isEmpty()) {
            stat.appendSQL(" ORDER BY ");
            int i = 0;
            for (OrderExpression<T> o : orderByList) {
                if (i++ > 0) {
                    stat.appendSQL(", ");
                }
                o.appendSQL(stat);
                stat.appendSQL(" ");
            }
        }
        if (limit > 0) {
            db.getDialect().appendLimit(stat, limit);
        }
        if (offset > 0) {
            db.getDialect().appendOffset(stat, offset);
        }
        StatementLogger.select(stat.getSQL());
    }

    /**
     * Join another table.
     *
     * @param alias an alias for the table to join
     * @return the joined query
     */
    @SuppressWarnings("unchecked")
    public <U> QueryJoin innerJoin(U alias) {
        TableDefinition<T> def = (TableDefinition<T>) db.define(alias
                .getClass());
        SelectTable<T> join = new SelectTable(db, this, alias, false);
        def.initSelectObject(join, alias, aliasMap);
        joins.add(join);
        return new QueryJoin(this, join);
    }

    Db getDb() {
        return db;
    }

    boolean isJoin() {
        return !joins.isEmpty();
    }

    SelectColumn<T> getSelectColumn(Object obj) {
        return aliasMap.get(obj);
    }

    void addOrderBy(OrderExpression<T> expr) {
        orderByList.add(expr);
    }

}
