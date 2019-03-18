/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.table.Table;

/**
 * This class represents the statement
 * TRUNCATE TABLE
 */
public class TruncateTable extends DefineCommand {

    private Table table;

    public TruncateTable(Session session) {
        super(session);
    }

    public void setTable(Table table) {
        this.table = table;
    }

    @Override
    public int update() {
        session.commit(true);
        if (!table.canTruncate()) {
            throw DbException.get(ErrorCode.CANNOT_TRUNCATE_1, table.getSQL());
        }
        session.getUser().checkRight(table, Right.DELETE);
        table.lock(session, true, true);
        table.truncate(session);
        return 0;
    }

    @Override
    public int getType() {
        return CommandInterface.TRUNCATE_TABLE;
    }

}
