/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.engine.UserDataType;
import org.h2.message.DbException;
import org.h2.table.Column;
import org.h2.table.Table;
import org.h2.value.DataType;

/**
 * This class represents the statement
 * CREATE DOMAIN
 */
public class CreateUserDataType extends DefineCommand {

    private String typeName;
    private Column column;
    private boolean ifNotExists;

    public CreateUserDataType(Session session) {
        super(session);
    }

    public void setTypeName(String name) {
        this.typeName = name;
    }

    public void setColumn(Column column) {
        this.column = column;
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    @Override
    public int update() {
        session.getUser().checkAdmin();
        session.commit(true);
        Database db = session.getDatabase();
        session.getUser().checkAdmin();
        if (db.findUserDataType(typeName) != null) {
            if (ifNotExists) {
                return 0;
            }
            throw DbException.get(
                    ErrorCode.USER_DATA_TYPE_ALREADY_EXISTS_1,
                    typeName);
        }
        DataType builtIn = DataType.getTypeByName(typeName, session.getDatabase().getMode());
        if (builtIn != null) {
            if (!builtIn.hidden) {
                throw DbException.get(
                        ErrorCode.USER_DATA_TYPE_ALREADY_EXISTS_1,
                        typeName);
            }
            Table table = session.getDatabase().getFirstUserTable();
            if (table != null) {
                throw DbException.get(
                        ErrorCode.USER_DATA_TYPE_ALREADY_EXISTS_1,
                        typeName + " (" + table.getSQL() + ")");
            }
        }
        int id = getObjectId();
        UserDataType type = new UserDataType(db, id, typeName);
        type.setColumn(column);
        db.addDatabaseObject(session, type);
        return 0;
    }

    @Override
    public int getType() {
        return CommandInterface.CREATE_DOMAIN;
    }

}
