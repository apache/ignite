/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import org.h2.api.ErrorCode;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.store.Data;
import org.h2.store.FileStore;
import org.h2.table.Table;
import org.h2.value.Value;

/**
 * An entry in a undo log.
 */
public class UndoLogRecord {

    /**
     * Operation type meaning the row was inserted.
     */
    public static final short INSERT = 0;

    /**
     * Operation type meaning the row was deleted.
     */
    public static final short DELETE = 1;

    private static final int IN_MEMORY = 0, STORED = 1, IN_MEMORY_INVALID = 2;
    private Table table;
    private Row row;
    private short operation;
    private short state;
    private int filePos;

    /**
     * Create a new undo log record
     *
     * @param table the table
     * @param op the operation type
     * @param row the row that was deleted or inserted
     */
    UndoLogRecord(Table table, short op, Row row) {
        this.table = table;
        this.row = row;
        this.operation = op;
        this.state = IN_MEMORY;
    }

    /**
     * Check if the log record is stored in the file.
     *
     * @return true if it is
     */
    boolean isStored() {
        return state == STORED;
    }

    /**
     * Check if this undo log record can be store. Only record can be stored if
     * the table has a unique index.
     *
     * @return if it can be stored
     */
    boolean canStore() {
        // if large transactions are enabled, this method is not called
        return table.getUniqueIndex() != null;
    }

    /**
     * Un-do the operation. If the row was inserted before, it is deleted now,
     * and vice versa.
     *
     * @param session the session
     */
    void undo(Session session) {
        Database db = session.getDatabase();
        switch (operation) {
        case INSERT:
            if (state == IN_MEMORY_INVALID) {
                state = IN_MEMORY;
            }
            if (db.getLockMode() == Constants.LOCK_MODE_OFF) {
                if (row.isDeleted()) {
                    // it might have been deleted by another thread
                    return;
                }
            }
            try {
                row.setDeleted(false);
                table.removeRow(session, row);
                table.fireAfterRow(session, row, null, true);
            } catch (DbException e) {
                if (session.getDatabase().getLockMode() == Constants.LOCK_MODE_OFF
                        && e.getErrorCode() == ErrorCode.ROW_NOT_FOUND_WHEN_DELETING_1) {
                    // it might have been deleted by another thread
                    // ignore
                } else {
                    throw e;
                }
            }
            break;
        case DELETE:
            try {
                table.addRow(session, row);
                table.fireAfterRow(session, null, row, true);
                // reset session id, otherwise other sessions think
                // that this row was inserted by this session
                row.commit();
            } catch (DbException e) {
                if (session.getDatabase().getLockMode() == Constants.LOCK_MODE_OFF
                        && e.getSQLException().getErrorCode() == ErrorCode.DUPLICATE_KEY_1) {
                    // it might have been added by another thread
                    // ignore
                } else {
                    throw e;
                }
            }
            break;
        default:
            DbException.throwInternalError("op=" + operation);
        }
    }

    /**
     * Append the row to the buffer.
     *
     * @param buff the buffer
     * @param log the undo log
     */
    void append(Data buff, UndoLog log) {
        int p = buff.length();
        buff.writeInt(0);
        buff.writeInt(operation);
        buff.writeByte(row.isDeleted() ? (byte) 1 : (byte) 0);
        buff.writeInt(log.getTableId(table));
        buff.writeLong(row.getKey());
        buff.writeInt(row.getSessionId());
        int count = row.getColumnCount();
        buff.writeInt(count);
        for (int i = 0; i < count; i++) {
            Value v = row.getValue(i);
            buff.checkCapacity(buff.getValueLen(v));
            buff.writeValue(v);
        }
        buff.fillAligned();
        buff.setInt(p, (buff.length() - p) / Constants.FILE_BLOCK_SIZE);
    }

    /**
     * Save the row in the file using a buffer.
     *
     * @param buff the buffer
     * @param file the file
     * @param log the undo log
     */
    void save(Data buff, FileStore file, UndoLog log) {
        buff.reset();
        append(buff, log);
        filePos = (int) (file.getFilePointer() / Constants.FILE_BLOCK_SIZE);
        file.write(buff.getBytes(), 0, buff.length());
        row = null;
        state = STORED;
    }

    /**
     * Load an undo log record row using a buffer.
     *
     * @param buff the buffer
     * @param log the log
     * @return the undo log record
     */
    static UndoLogRecord loadFromBuffer(Data buff, UndoLog log) {
        UndoLogRecord rec = new UndoLogRecord(null, (short) 0, null);
        int pos = buff.length();
        int len = buff.readInt() * Constants.FILE_BLOCK_SIZE;
        rec.load(buff, log);
        buff.setPos(pos + len);
        return rec;
    }

    /**
     * Load an undo log record row using a buffer.
     *
     * @param buff the buffer
     * @param file the source file
     * @param log the log
     */
    void load(Data buff, FileStore file, UndoLog log) {
        int min = Constants.FILE_BLOCK_SIZE;
        log.seek(filePos);
        buff.reset();
        file.readFully(buff.getBytes(), 0, min);
        int len = buff.readInt() * Constants.FILE_BLOCK_SIZE;
        buff.checkCapacity(len);
        if (len - min > 0) {
            file.readFully(buff.getBytes(), min, len - min);
        }
        int oldOp = operation;
        load(buff, log);
        if (SysProperties.CHECK) {
            if (operation != oldOp) {
                DbException.throwInternalError("operation=" + operation + " op=" + oldOp);
            }
        }
    }

    private void load(Data buff, UndoLog log) {
        operation = (short) buff.readInt();
        boolean deleted = buff.readByte() == 1;
        table = log.getTable(buff.readInt());
        long key = buff.readLong();
        int sessionId = buff.readInt();
        int columnCount = buff.readInt();
        Value[] values = new Value[columnCount];
        for (int i = 0; i < columnCount; i++) {
            values[i] = buff.readValue();
        }
        row = getTable().getDatabase().createRow(values, Row.MEMORY_CALCULATE);
        row.setKey(key);
        row.setDeleted(deleted);
        row.setSessionId(sessionId);
        state = IN_MEMORY_INVALID;
    }

    /**
     * Get the table.
     *
     * @return the table
     */
    public Table getTable() {
        return table;
    }

    /**
     * Get the position in the file.
     *
     * @return the file position
     */
    public long getFilePos() {
        return filePos;
    }

    /**
     * This method is called after the operation was committed.
     * It commits the change to the indexes.
     */
    void commit() {
        table.commit(operation, row);
    }

    /**
     * Get the row that was deleted or inserted.
     *
     * @return the row
     */
    public Row getRow() {
        return row;
    }

    /**
     * Change the state from IN_MEMORY to IN_MEMORY_INVALID. This method is
     * called if a later record was read from the temporary file, and therefore
     * the position could have changed.
     */
    void invalidatePos() {
        if (this.state == IN_MEMORY) {
            state = IN_MEMORY_INVALID;
        }
    }
}
