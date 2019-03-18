/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.constraint.Constraint;
import org.h2.constraint.ConstraintActionType;
import org.h2.engine.Database;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.schema.Schema;
import org.h2.table.Table;
import org.h2.table.TableView;
import org.h2.util.StatementBuilder;

/**
 * This class represents the statement
 * DROP TABLE
 */
public class DropTable extends SchemaCommand {

    private boolean ifExists;
    private String tableName;
    private Table table;
    private DropTable next;
    private ConstraintActionType dropAction;

    public DropTable(Session session, Schema schema) {
        super(session, schema);
        dropAction = session.getDatabase().getSettings().dropRestrict ?
                ConstraintActionType.RESTRICT :
                    ConstraintActionType.CASCADE;
    }

    /**
     * Chain another drop table statement to this statement.
     *
     * @param drop the statement to add
     */
    public void addNextDropTable(DropTable drop) {
        if (next == null) {
            next = drop;
        } else {
            next.addNextDropTable(drop);
        }
    }

    public void setIfExists(boolean b) {
        ifExists = b;
        if (next != null) {
            next.setIfExists(b);
        }
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    private void prepareDrop() {
        table = getSchema().findTableOrView(session, tableName);
        if (table == null) {
            if (!ifExists) {
                throw DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, tableName);
            }
        } else {
            session.getUser().checkRight(table, Right.ALL);
            if (!table.canDrop()) {
                throw DbException.get(ErrorCode.CANNOT_DROP_TABLE_1, tableName);
            }
            if (dropAction == ConstraintActionType.RESTRICT) {
                StatementBuilder buff = new StatementBuilder();
                CopyOnWriteArrayList<TableView> dependentViews = table.getDependentViews();
                if (dependentViews != null && !dependentViews.isEmpty()) {
                    for (TableView v : dependentViews) {
                        buff.appendExceptFirst(", ");
                        buff.append(v.getName());
                    }
                }
                if (session.getDatabase()
                        .getSettings().standardDropTableRestrict) {
                    final List<Constraint> constraints = table.getConstraints();
                    if (constraints != null && !constraints.isEmpty()) {
                        for (Constraint c : constraints) {
                            if (c.getTable() != table) {
                                buff.appendExceptFirst(", ");
                                buff.append(c.getName());
                            }
                        }
                    }
                }
                if (buff.length() > 0) {
                    throw DbException.get(ErrorCode.CANNOT_DROP_2, tableName, buff.toString());
                }

            }
            table.lock(session, true, true);
        }
        if (next != null) {
            next.prepareDrop();
        }
    }

    private void executeDrop() {
        // need to get the table again, because it may be dropped already
        // meanwhile (dependent object, or same object)
        table = getSchema().findTableOrView(session, tableName);

        if (table != null) {
            table.setModified();
            Database db = session.getDatabase();
            db.lockMeta(session);
            db.removeSchemaObject(session, table);
        }
        if (next != null) {
            next.executeDrop();
        }
    }

    @Override
    public int update() {
        session.commit(true);
        prepareDrop();
        executeDrop();
        return 0;
    }

    public void setDropAction(ConstraintActionType dropAction) {
        this.dropAction = dropAction;
        if (next != null) {
            next.setDropAction(dropAction);
        }
    }

    @Override
    public int getType() {
        return CommandInterface.DROP_TABLE;
    }

}
