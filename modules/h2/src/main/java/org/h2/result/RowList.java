/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import java.util.ArrayList;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.store.Data;
import org.h2.store.FileStore;
import org.h2.util.New;
import org.h2.value.Value;

/**
 * A list of rows. If the list grows too large, it is buffered to disk
 * automatically.
 */
public class RowList {

    private final Session session;
    private final ArrayList<Row> list = New.arrayList();
    private int size;
    private int index, listIndex;
    private FileStore file;
    private Data rowBuff;
    private ArrayList<Value> lobs;
    private final int maxMemory;
    private int memory;
    private boolean written;
    private boolean readUncached;

    /**
     * Construct a new row list for this session.
     *
     * @param session the session
     */
    public RowList(Session session) {
        this.session = session;
        if (session.getDatabase().isPersistent()) {
            maxMemory = session.getDatabase().getMaxOperationMemory();
        } else {
            maxMemory = 0;
        }
    }

    private void writeRow(Data buff, Row r) {
        buff.checkCapacity(1 + Data.LENGTH_INT * 8);
        buff.writeByte((byte) 1);
        buff.writeInt(r.getMemory());
        int columnCount = r.getColumnCount();
        buff.writeInt(columnCount);
        buff.writeLong(r.getKey());
        buff.writeInt(r.getVersion());
        buff.writeInt(r.isDeleted() ? 1 : 0);
        buff.writeInt(r.getSessionId());
        for (int i = 0; i < columnCount; i++) {
            Value v = r.getValue(i);
            buff.checkCapacity(1);
            if (v == null) {
                buff.writeByte((byte) 0);
            } else {
                buff.writeByte((byte) 1);
                if (v.getType() == Value.CLOB || v.getType() == Value.BLOB) {
                    // need to keep a reference to temporary lobs,
                    // otherwise the temp file is deleted
                    if (v.getSmall() == null && v.getTableId() == 0) {
                        if (lobs == null) {
                            lobs = New.arrayList();
                        }
                        // need to create a copy, otherwise,
                        // if stored multiple times, it may be renamed
                        // and then not found
                        v = v.copyToTemp();
                        lobs.add(v);
                    }
                }
                buff.checkCapacity(buff.getValueLen(v));
                buff.writeValue(v);
            }
        }
    }

    private void writeAllRows() {
        if (file == null) {
            Database db = session.getDatabase();
            String fileName = db.createTempFile();
            file = db.openFile(fileName, "rw", false);
            file.setCheckedWriting(false);
            file.seek(FileStore.HEADER_LENGTH);
            rowBuff = Data.create(db, Constants.DEFAULT_PAGE_SIZE);
            file.seek(FileStore.HEADER_LENGTH);
        }
        Data buff = rowBuff;
        initBuffer(buff);
        for (int i = 0, size = list.size(); i < size; i++) {
            if (i > 0 && buff.length() > Constants.IO_BUFFER_SIZE) {
                flushBuffer(buff);
                initBuffer(buff);
            }
            Row r = list.get(i);
            writeRow(buff, r);
        }
        flushBuffer(buff);
        file.autoDelete();
        list.clear();
        memory = 0;
    }

    private static void initBuffer(Data buff) {
        buff.reset();
        buff.writeInt(0);
    }

    private void flushBuffer(Data buff) {
        buff.checkCapacity(1);
        buff.writeByte((byte) 0);
        buff.fillAligned();
        buff.setInt(0, buff.length() / Constants.FILE_BLOCK_SIZE);
        file.write(buff.getBytes(), 0, buff.length());
    }

    /**
     * Add a row to the list.
     *
     * @param r the row to add
     */
    public void add(Row r) {
        list.add(r);
        memory += r.getMemory() + Constants.MEMORY_POINTER;
        if (maxMemory > 0 && memory > maxMemory) {
            writeAllRows();
        }
        size++;
    }

    /**
     * Remove all rows from the list.
     */
    public void reset() {
        index = 0;
        if (file != null) {
            listIndex = 0;
            if (!written) {
                writeAllRows();
                written = true;
            }
            list.clear();
            file.seek(FileStore.HEADER_LENGTH);
        }
    }

    /**
     * Check if there are more rows in this list.
     *
     * @return true it there are more rows
     */
    public boolean hasNext() {
        return index < size;
    }

    private Row readRow(Data buff) {
        if (buff.readByte() == 0) {
            return null;
        }
        int mem = buff.readInt();
        int columnCount = buff.readInt();
        long key = buff.readLong();
        int version = buff.readInt();
        if (readUncached) {
            key = 0;
        }
        boolean deleted = buff.readInt() == 1;
        int sessionId = buff.readInt();
        Value[] values = new Value[columnCount];
        for (int i = 0; i < columnCount; i++) {
            Value v;
            if (buff.readByte() == 0) {
                v = null;
            } else {
                v = buff.readValue();
                if (v.isLinkedToTable()) {
                    // the table id is 0 if it was linked when writing
                    // a temporary entry
                    if (v.getTableId() == 0) {
                        session.removeAtCommit(v);
                    }
                }
            }
            values[i] = v;
        }
        Row row = session.createRow(values, mem);
        row.setKey(key);
        row.setVersion(version);
        row.setDeleted(deleted);
        row.setSessionId(sessionId);
        return row;
    }

    /**
     * Get the next row from the list.
     *
     * @return the next row
     */
    public Row next() {
        Row r;
        if (file == null) {
            r = list.get(index++);
        } else {
            if (listIndex >= list.size()) {
                list.clear();
                listIndex = 0;
                Data buff = rowBuff;
                buff.reset();
                int min = Constants.FILE_BLOCK_SIZE;
                file.readFully(buff.getBytes(), 0, min);
                int len = buff.readInt() * Constants.FILE_BLOCK_SIZE;
                buff.checkCapacity(len);
                if (len - min > 0) {
                    file.readFully(buff.getBytes(), min, len - min);
                }
                while (true) {
                    r = readRow(buff);
                    if (r == null) {
                        break;
                    }
                    list.add(r);
                }
            }
            index++;
            r = list.get(listIndex++);
        }
        return r;
    }

    /**
     * Get the number of rows in this list.
     *
     * @return the number of rows
     */
    public int size() {
        return size;
    }

    /**
     * Do not use the cache.
     */
    public void invalidateCache() {
        readUncached = true;
    }

    /**
     * Close the result list and delete the temporary file.
     */
    public void close() {
        if (file != null) {
            file.autoDelete();
            file.closeAndDeleteSilently();
            file = null;
            rowBuff = null;
        }
    }

}
