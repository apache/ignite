/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import org.h2.message.DbException;
import org.h2.message.Trace;
import org.h2.table.Column;
import org.h2.table.Table;

/**
 * Represents a domain (user-defined data type).
 */
public class UserDataType extends DbObjectBase {

    private Column column;

    public UserDataType(Database database, int id, String name) {
        initDbObjectBase(database, id, name, Trace.DATABASE);
    }

    @Override
    public String getCreateSQLForCopy(Table table, String quotedName) {
        throw DbException.throwInternalError(toString());
    }

    @Override
    public String getDropSQL() {
        return "DROP DOMAIN IF EXISTS " + getSQL();
    }

    @Override
    public String getCreateSQL() {
        return "CREATE DOMAIN " + getSQL() + " AS " + column.getCreateSQL();
    }

    public Column getColumn() {
        return column;
    }

    @Override
    public int getType() {
        return DbObject.USER_DATATYPE;
    }

    @Override
    public void removeChildrenAndResources(Session session) {
        database.removeMeta(session, getId());
    }

    @Override
    public void checkRename() {
        // ok
    }

    public void setColumn(Column column) {
        this.column = column;
    }

}
