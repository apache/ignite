/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.util.ArrayList;
import java.util.HashMap;
import org.h2.message.DbException;
import org.h2.store.Data;
import org.h2.store.FileStore;
import org.h2.table.Table;
import org.h2.util.New;

/**
 * Each session keeps a undo log if rollback is required.
 */
public class UndoLog {

    private final Database database;
    private final ArrayList<Long> storedEntriesPos = New.arrayList();
    private final ArrayList<UndoLogRecord> records = New.arrayList();
    private FileStore file;
    private Data rowBuff;
    private int memoryUndo;
    private int storedEntries;
    private HashMap<Integer, Table> tables;
    private final boolean largeTransactions;

    /**
     * Create a new undo log for the given session.
     *
     * @param session the session
     */
    UndoLog(Session session) {
        this.database = session.getDatabase();
        largeTransactions = database.getSettings().largeTransactions;
    }

    /**
     * Get the number of active rows in this undo log.
     *
     * @return the number of rows
     */
    int size() {
        if (largeTransactions) {
            return storedEntries + records.size();
        }
        if (SysProperties.CHECK && memoryUndo > records.size()) {
            DbException.throwInternalError();
        }
        return records.size();
    }

    /**
     * Clear the undo log. This method is called after the transaction is
     * committed.
     */
    void clear() {
        records.clear();
        storedEntries = 0;
        storedEntriesPos.clear();
        memoryUndo = 0;
        if (file != null) {
            file.closeAndDeleteSilently();
            file = null;
            rowBuff = null;
        }
    }

    /**
     * Get the last record and remove it from the list of operations.
     *
     * @return the last record
     */
    public UndoLogRecord getLast() {
        int i = records.size() - 1;
        if (largeTransactions) {
            if (i < 0 && storedEntries > 0) {
                int last = storedEntriesPos.size() - 1;
                long pos = storedEntriesPos.remove(last);
                long end = file.length();
                int bufferLength = (int) (end - pos);
                Data buff = Data.create(database, bufferLength);
                file.seek(pos);
                file.readFully(buff.getBytes(), 0, bufferLength);
                while (buff.length() < bufferLength) {
                    UndoLogRecord e = UndoLogRecord.loadFromBuffer(buff, this);
                    records.add(e);
                    memoryUndo++;
                }
                storedEntries -= records.size();
                file.setLength(pos);
                file.seek(pos);
            }
            i = records.size() - 1;
        }
        UndoLogRecord entry = records.get(i);
        if (entry.isStored()) {
            int start = Math.max(0, i - database.getMaxMemoryUndo() / 2);
            UndoLogRecord first = null;
            for (int j = start; j <= i; j++) {
                UndoLogRecord e = records.get(j);
                if (e.isStored()) {
                    e.load(rowBuff, file, this);
                    memoryUndo++;
                    if (first == null) {
                        first = e;
                    }
                }
            }
            for (int k = 0; k < i; k++) {
                UndoLogRecord e = records.get(k);
                e.invalidatePos();
            }
            seek(first.getFilePos());
        }
        return entry;
    }

    /**
     * Go to the right position in the file.
     *
     * @param filePos the position in the file
     */
    void seek(long filePos) {
        file.seek(filePos * Constants.FILE_BLOCK_SIZE);
    }

    /**
     * Remove the last record from the list of operations.
     *
     * @param trimToSize if the undo array should shrink to conserve memory
     */
    void removeLast(boolean trimToSize) {
        int i = records.size() - 1;
        UndoLogRecord r = records.remove(i);
        if (!r.isStored()) {
            memoryUndo--;
        }
        if (trimToSize && i > 1024 && (i & 1023) == 0) {
            records.trimToSize();
        }
    }

    /**
     * Append an undo log entry to the log.
     *
     * @param entry the entry
     */
    void add(UndoLogRecord entry) {
        records.add(entry);
        if (largeTransactions) {
            memoryUndo++;
            if (memoryUndo > database.getMaxMemoryUndo() &&
                    database.isPersistent() &&
                    !database.isMultiVersion()) {
                if (file == null) {
                    String fileName = database.createTempFile();
                    file = database.openFile(fileName, "rw", false);
                    file.setCheckedWriting(false);
                    file.setLength(FileStore.HEADER_LENGTH);
                }
                Data buff = Data.create(database, Constants.DEFAULT_PAGE_SIZE);
                for (int i = 0; i < records.size(); i++) {
                    UndoLogRecord r = records.get(i);
                    buff.checkCapacity(Constants.DEFAULT_PAGE_SIZE);
                    r.append(buff, this);
                    if (i == records.size() - 1 || buff.length() > Constants.UNDO_BLOCK_SIZE) {
                        storedEntriesPos.add(file.getFilePointer());
                        file.write(buff.getBytes(), 0, buff.length());
                        buff.reset();
                    }
                }
                storedEntries += records.size();
                memoryUndo = 0;
                records.clear();
                file.autoDelete();
            }
        } else {
            if (!entry.isStored()) {
                memoryUndo++;
            }
            if (memoryUndo > database.getMaxMemoryUndo() &&
                    database.isPersistent() &&
                    !database.isMultiVersion()) {
                if (file == null) {
                    String fileName = database.createTempFile();
                    file = database.openFile(fileName, "rw", false);
                    file.setCheckedWriting(false);
                    file.seek(FileStore.HEADER_LENGTH);
                    rowBuff = Data.create(database, Constants.DEFAULT_PAGE_SIZE);
                    Data buff = rowBuff;
                    for (UndoLogRecord r : records) {
                        saveIfPossible(r, buff);
                    }
                } else {
                    saveIfPossible(entry, rowBuff);
                }
                file.autoDelete();
            }
        }
    }

    private void saveIfPossible(UndoLogRecord r, Data buff) {
        if (!r.isStored() && r.canStore()) {
            r.save(buff, file, this);
            memoryUndo--;
        }
    }

    /**
     * Get the table id for this undo log. If the table is not registered yet,
     * this is done as well.
     *
     * @param table the table
     * @return the id
     */
    int getTableId(Table table) {
        int id = table.getId();
        if (tables == null) {
            tables = new HashMap<>();
        }
        // need to overwrite the old entry, because the old object
        // might be deleted in the meantime
        tables.put(id, table);
        return id;
    }

    /**
     * Get the table for this id. The table must be registered for this undo log
     * first by calling getTableId.
     *
     * @param id the table id
     * @return the table object
     */
    Table getTable(int id) {
        return tables.get(id);
    }

}
