/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.util.ArrayList;
import org.h2.command.CommandInterface;
import org.h2.command.Prepared;
import org.h2.engine.Database;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.expression.Parameter;
import org.h2.result.ResultInterface;
import org.h2.table.Column;
import org.h2.table.Table;
import org.h2.table.TableType;
import org.h2.util.StatementBuilder;
import org.h2.value.Value;
import org.h2.value.ValueInt;
import org.h2.value.ValueNull;

/**
 * This class represents the statements
 * ANALYZE and ANALYZE TABLE
 */
public class Analyze extends DefineCommand {

    /**
     * The sample size.
     */
    private int sampleRows;
    /**
     * used in ANALYZE TABLE...
     */
    private Table table;

    public Analyze(Session session) {
        super(session);
        sampleRows = session.getDatabase().getSettings().analyzeSample;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    @Override
    public int update() {
        session.commit(true);
        session.getUser().checkAdmin();
        Database db = session.getDatabase();
        if (table != null) {
            analyzeTable(session, table, sampleRows, true);
        } else {
            for (Table table : db.getAllTablesAndViews(false)) {
                analyzeTable(session, table, sampleRows, true);
            }
        }
        return 0;
    }

    /**
     * Analyze this table.
     *
     * @param session the session
     * @param table the table
     * @param sample the number of sample rows
     * @param manual whether the command was called by the user
     */
    public static void analyzeTable(Session session, Table table, int sample,
                                    boolean manual) {
        if (table.getTableType() != TableType.TABLE ||
                table.isHidden() || session == null) {
            return;
        }
        if (!manual) {
            if (session.getDatabase().isSysTableLocked()) {
                return;
            }
            if (table.hasSelectTrigger()) {
                return;
            }
        }
        if (table.isTemporary() && !table.isGlobalTemporary()
                && session.findLocalTempTable(table.getName()) == null) {
            return;
        }
        if (table.isLockedExclusively() && !table.isLockedExclusivelyBy(session)) {
            return;
        }
        if (!session.getUser().hasRight(table, Right.SELECT)) {
            return;
        }
        if (session.getCancel() != 0) {
            // if the connection is closed and there is something to undo
            return;
        }
        Column[] columns = table.getColumns();
        if (columns.length == 0) {
            return;
        }
        Database db = session.getDatabase();
        StatementBuilder buff = new StatementBuilder("SELECT ");
        for (Column col : columns) {
            buff.appendExceptFirst(", ");
            int type = col.getType();
            if (type == Value.BLOB || type == Value.CLOB) {
                // can not index LOB columns, so calculating
                // the selectivity is not required
                buff.append("MAX(NULL)");
            } else {
                buff.append("SELECTIVITY(").append(col.getSQL()).append(')');
            }
        }
        buff.append(" FROM ").append(table.getSQL());
        if (sample > 0) {
            buff.append(" LIMIT ? SAMPLE_SIZE ? ");
        }
        String sql = buff.toString();
        Prepared command = session.prepare(sql);
        if (sample > 0) {
            ArrayList<Parameter> params = command.getParameters();
            params.get(0).setValue(ValueInt.get(1));
            params.get(1).setValue(ValueInt.get(sample));
        }
        ResultInterface result = command.query(0);
        result.next();
        for (int j = 0; j < columns.length; j++) {
            Value v = result.currentRow()[j];
            if (v != ValueNull.INSTANCE) {
                int selectivity = v.getInt();
                columns[j].setSelectivity(selectivity);
            }
        }
        db.updateMeta(session, table);
    }

    public void setTop(int top) {
        this.sampleRows = top;
    }

    @Override
    public int getType() {
        return CommandInterface.ANALYZE;
    }

}
