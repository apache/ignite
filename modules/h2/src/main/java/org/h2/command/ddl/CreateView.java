/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.util.ArrayList;
import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.command.dml.Query;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.expression.Parameter;
import org.h2.message.DbException;
import org.h2.schema.Schema;
import org.h2.table.Column;
import org.h2.table.Table;
import org.h2.table.TableType;
import org.h2.table.TableView;
import org.h2.value.Value;

/**
 * This class represents the statement
 * CREATE VIEW
 */
public class CreateView extends SchemaCommand {

    private Query select;
    private String viewName;
    private boolean ifNotExists;
    private String selectSQL;
    private String[] columnNames;
    private String comment;
    private boolean orReplace;
    private boolean force;
    private boolean isTableExpression;

    public CreateView(Session session, Schema schema) {
        super(session, schema);
    }

    public void setViewName(String name) {
        viewName = name;
    }

    public void setSelect(Query select) {
        this.select = select;
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public void setSelectSQL(String selectSQL) {
        this.selectSQL = selectSQL;
    }

    public void setColumnNames(String[] cols) {
        this.columnNames = cols;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public void setOrReplace(boolean orReplace) {
        this.orReplace = orReplace;
    }

    public void setForce(boolean force) {
        this.force = force;
    }

    public void setTableExpression(boolean isTableExpression) {
        this.isTableExpression = isTableExpression;
    }

    @Override
    public int update() {
        session.commit(true);
        session.getUser().checkAdmin();
        Database db = session.getDatabase();
        TableView view = null;
        Table old = getSchema().findTableOrView(session, viewName);
        if (old != null) {
            if (ifNotExists) {
                return 0;
            }
            if (!orReplace || TableType.VIEW != old.getTableType()) {
                throw DbException.get(ErrorCode.VIEW_ALREADY_EXISTS_1, viewName);
            }
            view = (TableView) old;
        }
        int id = getObjectId();
        String querySQL;
        if (select == null) {
            querySQL = selectSQL;
        } else {
            ArrayList<Parameter> params = select.getParameters();
            if (params != null && !params.isEmpty()) {
                throw DbException.getUnsupportedException("parameters in views");
            }
            querySQL = select.getPlanSQL();
        }
        Column[] columnTemplatesAsUnknowns = null;
        Column[] columnTemplatesAsStrings = null;
        if (columnNames != null) {
            columnTemplatesAsUnknowns = new Column[columnNames.length];
            columnTemplatesAsStrings = new Column[columnNames.length];
            for (int i = 0; i < columnNames.length; ++i) {
                // non table expressions are fine to use unknown column type
                columnTemplatesAsUnknowns[i] = new Column(columnNames[i], Value.UNKNOWN);
                // table expressions can't have unknown types - so we use string instead
                columnTemplatesAsStrings[i] = new Column(columnNames[i], Value.STRING);
            }
        }
        if (view == null) {
            if (isTableExpression) {
                view = TableView.createTableViewMaybeRecursive(getSchema(), id, viewName, querySQL, null,
                        columnTemplatesAsStrings, session, false /* literalsChecked */, isTableExpression,
                        true /* isPersistent */, db);
            } else {
                view = new TableView(getSchema(), id, viewName, querySQL, null, columnTemplatesAsUnknowns, session,
                        false/* allow recursive */, false/* literalsChecked */, isTableExpression, true);
            }
        } else {
            // TODO support isTableExpression in replace function...
            view.replace(querySQL, columnTemplatesAsUnknowns, session, false, force, false);
            view.setModified();
        }
        if (comment != null) {
            view.setComment(comment);
        }
        if (old == null) {
            db.addSchemaObject(session, view);
            db.unlockMeta(session);
        } else {
            db.updateMeta(session, view);
        }

        // TODO: if we added any table expressions that aren't used by this view, detect them
        // and drop them - otherwise they will leak and never get cleaned up.

        return 0;
    }

    @Override
    public int getType() {
        return CommandInterface.CREATE_VIEW;
    }

}
