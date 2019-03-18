/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.util.ArrayList;
import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.constraint.ConstraintActionType;
import org.h2.engine.DbObject;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.schema.Schema;
import org.h2.table.Table;
import org.h2.table.TableType;
import org.h2.table.TableView;

/**
 * This class represents the statement
 * DROP VIEW
 */
public class DropView extends SchemaCommand {

    private String viewName;
    private boolean ifExists;
    private ConstraintActionType dropAction;

    public DropView(Session session, Schema schema) {
        super(session, schema);
        dropAction = session.getDatabase().getSettings().dropRestrict ?
                ConstraintActionType.RESTRICT :
                ConstraintActionType.CASCADE;
    }

    public void setIfExists(boolean b) {
        ifExists = b;
    }

    public void setDropAction(ConstraintActionType dropAction) {
        this.dropAction = dropAction;
    }

    public void setViewName(String viewName) {
        this.viewName = viewName;
    }

    @Override
    public int update() {
        session.commit(true);
        Table view = getSchema().findTableOrView(session, viewName);
        if (view == null) {
            if (!ifExists) {
                throw DbException.get(ErrorCode.VIEW_NOT_FOUND_1, viewName);
            }
        } else {
            if (TableType.VIEW != view.getTableType()) {
                throw DbException.get(ErrorCode.VIEW_NOT_FOUND_1, viewName);
            }
            session.getUser().checkRight(view, Right.ALL);

            if (dropAction == ConstraintActionType.RESTRICT) {
                for (DbObject child : view.getChildren()) {
                    if (child instanceof TableView) {
                        throw DbException.get(ErrorCode.CANNOT_DROP_2, viewName, child.getName());
                    }
                }
            }

            // TODO: Where is the ConstraintReferential.CASCADE style drop processing ? It's
            // supported from imported keys - but not for dependent db objects

            TableView tableView = (TableView) view;
            ArrayList<Table> copyOfDependencies = new ArrayList<>(tableView.getTables());

            view.lock(session, true, true);
            session.getDatabase().removeSchemaObject(session, view);

            // remove dependent table expressions
            for (Table childTable: copyOfDependencies) {
                if (TableType.VIEW == childTable.getTableType()) {
                    TableView childTableView = (TableView) childTable;
                    if (childTableView.isTableExpression() && childTableView.getName() != null) {
                        session.getDatabase().removeSchemaObject(session, childTableView);
                    }
                }
            }
            // make sure its all unlocked
            session.getDatabase().unlockMeta(session);
        }
        return 0;
    }

    @Override
    public int getType() {
        return CommandInterface.DROP_VIEW;
    }

}
