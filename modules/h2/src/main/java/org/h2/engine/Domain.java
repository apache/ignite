/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import org.h2.message.DbException;
import org.h2.message.Trace;
import org.h2.table.Column;
import org.h2.table.Table;

/**
 * Represents a domain.
 */
public class Domain extends DbObjectBase {

    private Column column;

    public Domain(Database database, int id, String name) {
        super(database, id, name, Trace.DATABASE);
    }

    @Override
    public String getCreateSQLForCopy(Table table, String quotedName) {
        throw DbException.throwInternalError(toString());
    }

    @Override
    public String getDropSQL() {
        StringBuilder builder = new StringBuilder("DROP DOMAIN IF EXISTS ");
        return getSQL(builder, true).toString();
    }

    @Override
    public String getCreateSQL() {
        StringBuilder builder = new StringBuilder("CREATE DOMAIN ");
        getSQL(builder, true).append(" AS ");
        builder.append(column.getCreateSQL());
        return builder.toString();
    }

    public Column getColumn() {
        return column;
    }

    @Override
    public int getType() {
        return DbObject.DOMAIN;
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
