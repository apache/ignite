/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.util.ArrayList;
import java.util.HashSet;
import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.command.dml.Insert;
import org.h2.command.dml.Query;
import org.h2.engine.Database;
import org.h2.engine.DbObject;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.message.DbException;
import org.h2.schema.Schema;
import org.h2.schema.Sequence;
import org.h2.table.Column;
import org.h2.table.Table;
import org.h2.util.ColumnNamer;
import org.h2.value.DataType;
import org.h2.value.Value;

/**
 * This class represents the statement
 * CREATE TABLE
 */
public class CreateTable extends CommandWithColumns {

    private final CreateTableData data = new CreateTableData();
    private boolean ifNotExists;
    private boolean onCommitDrop;
    private boolean onCommitTruncate;
    private Query asQuery;
    private String comment;
    private boolean sortedInsertMode;

    public CreateTable(Session session, Schema schema) {
        super(session, schema);
        data.persistIndexes = true;
        data.persistData = true;
    }

    public void setQuery(Query query) {
        this.asQuery = query;
    }

    public void setTemporary(boolean temporary) {
        data.temporary = temporary;
    }

    public void setTableName(String tableName) {
        data.tableName = tableName;
    }

    @Override
    public void addColumn(Column column) {
        data.columns.add(column);
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    @Override
    public int update() {
        if (!transactional) {
            session.commit(true);
        }
        Database db = session.getDatabase();
        if (!db.isPersistent()) {
            data.persistIndexes = false;
        }
        boolean isSessionTemporary = data.temporary && !data.globalTemporary;
        if (!isSessionTemporary) {
            db.lockMeta(session);
        }
        if (getSchema().resolveTableOrView(session, data.tableName) != null) {
            if (ifNotExists) {
                return 0;
            }
            throw DbException.get(ErrorCode.TABLE_OR_VIEW_ALREADY_EXISTS_1, data.tableName);
        }
        if (asQuery != null) {
            asQuery.prepare();
            if (data.columns.isEmpty()) {
                generateColumnsFromQuery();
            } else if (data.columns.size() != asQuery.getColumnCount()) {
                throw DbException.get(ErrorCode.COLUMN_COUNT_DOES_NOT_MATCH);
            }
        }
        changePrimaryKeysToNotNull(data.columns);
        data.id = getObjectId();
        data.create = create;
        data.session = session;
        Table table = getSchema().createTable(data);
        ArrayList<Sequence> sequences = generateSequences(data.columns, data.temporary);
        table.setComment(comment);
        if (isSessionTemporary) {
            if (onCommitDrop) {
                table.setOnCommitDrop(true);
            }
            if (onCommitTruncate) {
                table.setOnCommitTruncate(true);
            }
            session.addLocalTempTable(table);
        } else {
            db.lockMeta(session);
            db.addSchemaObject(session, table);
        }
        try {
            for (Column c : data.columns) {
                c.prepareExpression(session);
            }
            for (Sequence sequence : sequences) {
                table.addSequence(sequence);
            }
            createConstraints();
            if (asQuery != null) {
                boolean old = session.isUndoLogEnabled();
                try {
                    session.setUndoLogEnabled(false);
                    session.startStatementWithinTransaction();
                    Insert insert = new Insert(session);
                    insert.setSortedInsertMode(sortedInsertMode);
                    insert.setQuery(asQuery);
                    insert.setTable(table);
                    insert.setInsertFromSelect(true);
                    insert.prepare();
                    insert.update();
                } finally {
                    session.setUndoLogEnabled(old);
                }
            }
            HashSet<DbObject> set = new HashSet<>();
            set.clear();
            table.addDependencies(set);
            for (DbObject obj : set) {
                if (obj == table) {
                    continue;
                }
                if (obj.getType() == DbObject.TABLE_OR_VIEW) {
                    if (obj instanceof Table) {
                        Table t = (Table) obj;
                        if (t.getId() > table.getId()) {
                            throw DbException.get(
                                    ErrorCode.FEATURE_NOT_SUPPORTED_1,
                                    "Table depends on another table " +
                                    "with a higher ID: " + t +
                                    ", this is currently not supported, " +
                                    "as it would prevent the database from " +
                                    "being re-opened");
                        }
                    }
                }
            }
        } catch (DbException e) {
            db.checkPowerOff();
            db.removeSchemaObject(session, table);
            if (!transactional) {
                session.commit(true);
            }
            throw e;
        }
        return 0;
    }

    private void generateColumnsFromQuery() {
        int columnCount = asQuery.getColumnCount();
        ArrayList<Expression> expressions = asQuery.getExpressions();
        ColumnNamer columnNamer= new ColumnNamer(session);
        for (int i = 0; i < columnCount; i++) {
            Expression expr = expressions.get(i);
            int type = expr.getType();
            String name = columnNamer.getColumnName(expr,i,expr.getAlias());
            long precision = expr.getPrecision();
            int displaySize = expr.getDisplaySize();
            DataType dt = DataType.getDataType(type);
            if (precision > 0 && (dt.defaultPrecision == 0 ||
                    (dt.defaultPrecision > precision && dt.defaultPrecision < Byte.MAX_VALUE))) {
                // dont' set precision to MAX_VALUE if this is the default
                precision = dt.defaultPrecision;
            }
            int scale = expr.getScale();
            if (scale > 0 && (dt.defaultScale == 0 ||
                    (dt.defaultScale > scale && dt.defaultScale < precision))) {
                scale = dt.defaultScale;
            }
            if (scale > precision) {
                precision = scale;
            }
            String[] enumerators = null;
            if (dt.type == Value.ENUM) {
                /**
                 * Only columns of tables may be enumerated.
                 */
                if(!(expr instanceof ExpressionColumn)) {
                    throw DbException.get(ErrorCode.GENERAL_ERROR_1,
                            "Unable to resolve enumerators of expression");
                }
                enumerators = ((ExpressionColumn)expr).getColumn().getEnumerators();
            }
            Column col = new Column(name, type, precision, scale, displaySize, enumerators);
            addColumn(col);
        }
    }

    public void setPersistIndexes(boolean persistIndexes) {
        data.persistIndexes = persistIndexes;
    }

    public void setGlobalTemporary(boolean globalTemporary) {
        data.globalTemporary = globalTemporary;
    }

    /**
     * This temporary table is dropped on commit.
     */
    public void setOnCommitDrop() {
        this.onCommitDrop = true;
    }

    /**
     * This temporary table is truncated on commit.
     */
    public void setOnCommitTruncate() {
        this.onCommitTruncate = true;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public void setPersistData(boolean persistData) {
        data.persistData = persistData;
        if (!persistData) {
            data.persistIndexes = false;
        }
    }

    public void setSortedInsertMode(boolean sortedInsertMode) {
        this.sortedInsertMode = sortedInsertMode;
    }

    public void setTableEngine(String tableEngine) {
        data.tableEngine = tableEngine;
    }

    public void setTableEngineParams(ArrayList<String> tableEngineParams) {
        data.tableEngineParams = tableEngineParams;
    }

    public void setHidden(boolean isHidden) {
        data.isHidden = isHidden;
    }

    @Override
    public int getType() {
        return CommandInterface.CREATE_TABLE;
    }

}
