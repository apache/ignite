/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.sql.Connection;
import java.sql.SQLException;
import org.h2.api.Aggregate;
import org.h2.api.AggregateFunction;
import org.h2.command.Parser;
import org.h2.message.DbException;
import org.h2.message.Trace;
import org.h2.table.Table;
import org.h2.util.JdbcUtils;
import org.h2.value.DataType;

/**
 * Represents a user-defined aggregate function.
 */
public class UserAggregate extends DbObjectBase {

    private String className;
    private Class<?> javaClass;

    public UserAggregate(Database db, int id, String name, String className,
            boolean force) {
        initDbObjectBase(db, id, name, Trace.FUNCTION);
        this.className = className;
        if (!force) {
            getInstance();
        }
    }

    public Aggregate getInstance() {
        if (javaClass == null) {
            javaClass = JdbcUtils.loadUserClass(className);
        }
        Object obj;
        try {
            obj = javaClass.newInstance();
            Aggregate agg;
            if (obj instanceof Aggregate) {
                agg = (Aggregate) obj;
            } else {
                agg = new AggregateWrapper((AggregateFunction) obj);
            }
            return agg;
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }

    @Override
    public String getCreateSQLForCopy(Table table, String quotedName) {
        throw DbException.throwInternalError(toString());
    }

    @Override
    public String getDropSQL() {
        return "DROP AGGREGATE IF EXISTS " + getSQL();
    }

    @Override
    public String getCreateSQL() {
        return "CREATE FORCE AGGREGATE " + getSQL() +
                " FOR " + Parser.quoteIdentifier(className);
    }

    @Override
    public int getType() {
        return DbObject.AGGREGATE;
    }

    @Override
    public synchronized void removeChildrenAndResources(Session session) {
        database.removeMeta(session, getId());
        className = null;
        javaClass = null;
        invalidate();
    }

    @Override
    public void checkRename() {
        throw DbException.getUnsupportedException("AGGREGATE");
    }

    public String getJavaClassName() {
        return this.className;
    }

    /**
     * Wrap {@link AggregateFunction} in order to behave as
     * {@link org.h2.api.Aggregate}
     **/
    private static class AggregateWrapper implements Aggregate {
        private final AggregateFunction aggregateFunction;

        AggregateWrapper(AggregateFunction aggregateFunction) {
            this.aggregateFunction = aggregateFunction;
        }

        @Override
        public void init(Connection conn) throws SQLException {
            aggregateFunction.init(conn);
        }

        @Override
        public int getInternalType(int[] inputTypes) throws SQLException {
            int[] sqlTypes = new int[inputTypes.length];
            for (int i = 0; i < inputTypes.length; i++) {
                sqlTypes[i] = DataType.convertTypeToSQLType(inputTypes[i]);
            }
            return  DataType.convertSQLTypeToValueType(aggregateFunction.getType(sqlTypes));
        }

        @Override
        public void add(Object value) throws SQLException {
            aggregateFunction.add(value);
        }

        @Override
        public Object getResult() throws SQLException {
            return aggregateFunction.getResult();
        }
    }

}
