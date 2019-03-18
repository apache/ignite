/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import org.h2.command.CommandInterface;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.table.TableView;

/**
 * This class represents the statement
 * ALTER VIEW
 */
public class AlterView extends DefineCommand {

    private boolean ifExists;
    private TableView view;

    public AlterView(Session session) {
        super(session);
    }

    public void setIfExists(boolean b) {
        ifExists = b;
    }

    public void setView(TableView view) {
        this.view = view;
    }

    @Override
    public int update() {
        session.commit(true);
        if (view == null && ifExists) {
            return 0;
        }
        session.getUser().checkRight(view, Right.ALL);
        DbException e = view.recompile(session, false, true);
        if (e != null) {
            throw e;
        }
        return 0;
    }

    @Override
    public int getType() {
        return CommandInterface.ALTER_VIEW;
    }

}
