/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import org.h2.message.DbException;
import org.h2.message.Trace;
import org.h2.table.Table;

/**
 * A persistent database setting.
 */
public class Setting extends DbObjectBase {

    private int intValue;
    private String stringValue;

    public Setting(Database database, int id, String settingName) {
        initDbObjectBase(database, id, settingName, Trace.SETTING);
    }

    public void setIntValue(int value) {
        intValue = value;
    }

    public int getIntValue() {
        return intValue;
    }

    public void setStringValue(String value) {
        stringValue = value;
    }

    public String getStringValue() {
        return stringValue;
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
        StringBuilder buff = new StringBuilder("SET ");
        buff.append(getSQL()).append(' ');
        if (stringValue != null) {
            buff.append(stringValue);
        } else {
            buff.append(intValue);
        }
        return buff.toString();
    }

    @Override
    public int getType() {
        return DbObject.SETTING;
    }

    @Override
    public void removeChildrenAndResources(Session session) {
        database.removeMeta(session, getId());
        invalidate();
    }

    @Override
    public void checkRename() {
        throw DbException.getUnsupportedException("RENAME");
    }

}
