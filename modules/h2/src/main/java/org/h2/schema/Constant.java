/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.schema;

import org.h2.engine.DbObject;
import org.h2.engine.Session;
import org.h2.expression.ValueExpression;
import org.h2.message.DbException;
import org.h2.message.Trace;
import org.h2.table.Table;
import org.h2.value.Value;

/**
 * A user-defined constant as created by the SQL statement
 * CREATE CONSTANT
 */
public class Constant extends SchemaObjectBase {

    private Value value;
    private ValueExpression expression;

    public Constant(Schema schema, int id, String name) {
        initSchemaObjectBase(schema, id, name, Trace.SCHEMA);
    }

    @Override
    public String getCreateSQLForCopy(Table table, String quotedName) {
        throw DbException.throwInternalError(toString());
    }

    @Override
    public String getDropSQL() {
        return null;
    }

    @Override
    public String getCreateSQL() {
        return "CREATE CONSTANT " + getSQL() + " VALUE " + value.getSQL();
    }

    @Override
    public int getType() {
        return DbObject.CONSTANT;
    }

    @Override
    public void removeChildrenAndResources(Session session) {
        database.removeMeta(session, getId());
        invalidate();
    }

    @Override
    public void checkRename() {
        // ok
    }

    public void setValue(Value value) {
        this.value = value;
        expression = ValueExpression.get(value);
    }

    public ValueExpression getValue() {
        return expression;
    }

}
