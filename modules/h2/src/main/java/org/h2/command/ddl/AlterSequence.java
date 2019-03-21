/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.schema.Schema;
import org.h2.schema.Sequence;
import org.h2.table.Column;
import org.h2.table.Table;

/**
 * This class represents the statement ALTER SEQUENCE.
 */
public class AlterSequence extends SchemaCommand {

    private boolean ifExists;

    private Table table;

    private String sequenceName;

    private Sequence sequence;

    private SequenceOptions options;

    public AlterSequence(Session session, Schema schema) {
        super(session, schema);
    }

    public void setIfExists(boolean b) {
        ifExists = b;
    }

    public void setSequenceName(String sequenceName) {
        this.sequenceName = sequenceName;
    }

    public void setOptions(SequenceOptions options) {
        this.options = options;
    }

    @Override
    public boolean isTransactional() {
        return true;
    }

    public void setColumn(Column column) {
        table = column.getTable();
        sequence = column.getSequence();
        if (sequence == null && !ifExists) {
            throw DbException.get(ErrorCode.SEQUENCE_NOT_FOUND_1, column.getSQL(false));
        }
    }

    @Override
    public int update() {
        if (sequence == null) {
            sequence = getSchema().findSequence(sequenceName);
            if (sequence == null) {
                if (!ifExists) {
                    throw DbException.get(ErrorCode.SEQUENCE_NOT_FOUND_1, sequenceName);
                }
                return 0;
            }
        }
        if (table != null) {
            session.getUser().checkRight(table, Right.ALL);
        }
        Boolean cycle = options.getCycle();
        if (cycle != null) {
            sequence.setCycle(cycle);
        }
        Long cache = options.getCacheSize(session);
        if (cache != null) {
            sequence.setCacheSize(cache);
        }
        if (options.isRangeSet()) {
            sequence.modify(options.getStartValue(session), options.getMinValue(sequence, session),
                    options.getMaxValue(sequence, session), options.getIncrement(session));
        }
        sequence.flush(session);
        return 0;
    }

    @Override
    public int getType() {
        return CommandInterface.ALTER_SEQUENCE;
    }

}
